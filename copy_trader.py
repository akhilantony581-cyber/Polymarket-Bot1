"""
copy_trader.py  —  Bot 3
Mirrors trades from a target Polymarket wallet.

Flow (every 5s):
  1. Poll data-api.polymarket.com/activity for the target wallet
  2. For each new BUY trade not yet seen:
     - Skip if price < min_price (0.89)
     - Skip if market exposure already at max_per_market ($100)
     - Apply Binance momentum gate (same config as bot1/bot2)
     - Fetch market details from gamma API
     - Place a limit order for the exact usdcSize the target placed
"""

import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Optional, Set

import httpx

from binance_feed import BinanceFeed
from order_manager import OrderManager
from polymarket_listener import OrderBook, PolymarketMarket

logger = logging.getLogger(__name__)

# Title keyword → coin ticker
_COIN_MAP = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "xrp": "XRP",
    "ripple": "XRP",
    "dogecoin": "DOGE",
    "doge": "DOGE",
    "bnb": "BNB",
    "binance coin": "BNB",
    "hype": "HYPE",
    "hyperliquid": "HYPE",
}

_ACTIVITY_URL = "https://data-api.polymarket.com/activity"
_GAMMA_URL    = "https://gamma-api.polymarket.com/markets"


class CopyTrader:
    """
    Bot 3: copies trades from a target Polymarket wallet in real time.
    Shares the OrderManager (and capital) with bot1 and bot2.
    """

    def __init__(
        self,
        config: dict,
        order_manager: OrderManager,
        binance: BinanceFeed,
        on_fill=None,
    ):
        cfg = config.get("copy_trader", {})
        self.target_wallet: str  = cfg.get("target_wallet", "").lower()
        self.min_price: float    = cfg.get("min_price", 0.89)
        self.max_per_market: float = cfg.get("max_per_market", 100.0)
        self.poll_interval: float  = cfg.get("poll_interval_seconds", 5.0)

        self.order_manager = order_manager
        self.binance        = binance
        self.config         = config
        self.on_fill        = on_fill  # callback so structured_logger.log_trade_open is called

        self._running    = False
        self._start_ts   = 0.0
        self._seen_tx: Set[str]          = set()
        self._exposure:  Dict[str, float] = defaultdict(float)   # conditionId → USDC placed
        self._mkt_cache: Dict[str, PolymarketMarket] = {}
        self._client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        self._running  = True
        self._start_ts = time.time()
        self._client   = httpx.AsyncClient(timeout=10.0)
        asyncio.create_task(self._run())
        logger.info(
            f"CopyTrader (bot3) started — mirroring {self.target_wallet[:20]}... "
            f"min_price={self.min_price} max_per_market=${self.max_per_market}"
        )

    async def stop(self):
        self._running = False
        if self._client:
            await self._client.aclose()

    async def _run(self):
        while self._running:
            try:
                await self._poll_once()
            except Exception as e:
                logger.error(f"CopyTrader poll error: {e}", exc_info=True)
            await asyncio.sleep(self.poll_interval)

    # ------------------------------------------------------------------
    # Poll & process
    # ------------------------------------------------------------------

    async def _poll_once(self):
        try:
            resp = await self._client.get(
                _ACTIVITY_URL,
                params={"user": self.target_wallet, "limit": 20},
            )
        except Exception as e:
            logger.debug(f"CopyTrader activity fetch error: {e}")
            return

        if resp.status_code != 200:
            return

        items = resp.json() or []
        for item in reversed(items):   # oldest first so we process in order
            if item.get("type") != "TRADE" or item.get("side") != "BUY":
                continue
            tx = item.get("transactionHash", "")
            if not tx or tx in self._seen_tx:
                continue
            ts = float(item.get("timestamp", 0))
            self._seen_tx.add(tx)
            if ts < self._start_ts:
                continue                              # historical — ignore
            await self._maybe_copy(item)

    async def _maybe_copy(self, item: dict):
        price        = float(item.get("price", 0))
        usdc_size    = float(item.get("usdcSize", 0))
        token_id     = str(item.get("asset", ""))
        condition_id = item.get("conditionId", "")
        title        = item.get("title", "")
        outcome      = item.get("outcome", "")   # "Up" / "Down"

        # --- Basic filters ---
        if price < self.min_price:
            logger.debug(f"CopyTrader skip: price {price:.3f} < min {self.min_price}")
            return
        if usdc_size <= 0 or not token_id or not condition_id:
            return

        # --- Market exposure cap ---
        remaining = self.max_per_market - self._exposure[condition_id]
        if remaining <= 1.0:
            logger.debug(f"CopyTrader skip: {condition_id[:12]} already at exposure cap")
            return
        usdc_size = min(usdc_size, remaining)

        # --- Coin detection ---
        coin = _extract_coin(title)
        if not coin:
            logger.debug(f"CopyTrader skip: unrecognised coin in '{title}'")
            return

        # --- Binance momentum gate ---
        mg = self.config.get("momentum_gate", {})
        if mg.get("enabled", True) and not self._momentum_ok(coin, outcome, mg):
            logger.info(
                f"CopyTrader momentum gate blocked: {coin} {outcome} "
                f"price={price:.3f} usdc={usdc_size:.2f}"
            )
            return

        # --- Fetch/build market object ---
        market = await self._get_market(condition_id, token_id, coin, outcome, price, title)
        if not market:
            logger.warning(f"CopyTrader: could not resolve market for {condition_id[:16]}")
            return

        # Skip if market is already expired or expiring in < 15s
        if market.seconds_to_expiry < 15:
            logger.debug(f"CopyTrader skip: market {condition_id[:12]} expires in {market.seconds_to_expiry:.0f}s")
            return

        logger.info(
            f"CopyTrader copying: {coin} {outcome} price={price:.3f} "
            f"usdc=${usdc_size:.2f} market={condition_id[:16]}"
        )

        pos = await self.order_manager.submit(market, price, usdc_size, mode="copy")
        if pos:
            self._exposure[condition_id] += usdc_size
            if self.on_fill:
                self.on_fill(pos)

    # ------------------------------------------------------------------
    # Binance momentum gate
    # ------------------------------------------------------------------

    def _momentum_ok(self, coin: str, outcome: str, mg: dict) -> bool:
        """
        Directional check: 'Up' positions require non-sharply-negative momentum,
        'Down' positions require non-sharply-positive momentum.
        """
        bd = self.binance.get(coin)
        if not bd or not self.binance.is_ready(coin):
            return True   # no data — allow
        mom = bd.momentum(mg.get("window_seconds", 30))
        if mom is None:
            return True
        boundary = mg.get("boundary_pct", 0.03)
        is_up = outcome.lower() in ("up", "yes", "above")
        if is_up and mom < -boundary:
            return False   # price falling sharply — don't chase up
        if not is_up and mom > boundary:
            return False   # price rising sharply — don't chase down
        return True

    # ------------------------------------------------------------------
    # Market resolution
    # ------------------------------------------------------------------

    async def _get_market(
        self,
        condition_id: str,
        token_id: str,
        coin: str,
        outcome: str,
        price: float,
        title: str,
    ) -> Optional[PolymarketMarket]:
        # Serve from cache (update price)
        if condition_id in self._mkt_cache:
            m = self._mkt_cache[condition_id]
            m.yes_price = price
            return m

        try:
            resp = await self._client.get(
                _GAMMA_URL,
                params={"condition_id": condition_id},
                timeout=8.0,
            )
            if resp.status_code != 200:
                return None
            raw = resp.json()
            items = raw if isinstance(raw, list) else [raw]
            if not items:
                return None
            md = items[0]

            tokens    = md.get("tokens") or md.get("clob_token_ids", [])
            # tokens is either a list of dicts with token_id/outcome,
            # or a list of raw token id strings (two entries: yes/no)
            yes_token_id, no_token_id = token_id, ""
            if tokens and isinstance(tokens[0], dict):
                other = next(
                    (t for t in tokens if str(t.get("token_id", "")) != token_id), None
                )
                no_token_id = str(other.get("token_id", "")) if other else ""
            elif isinstance(tokens, list) and len(tokens) == 2:
                no_token_id = str(tokens[1]) if str(tokens[0]) == token_id else str(tokens[0])

            # Parse expiry timestamp
            end_raw = md.get("end_date_iso") or md.get("endDateIso") or md.get("end_date") or ""
            expiry_ts = 0.0
            if end_raw:
                try:
                    expiry_ts = datetime.fromisoformat(
                        end_raw.replace("Z", "+00:00")
                    ).astimezone(timezone.utc).timestamp()
                except Exception:
                    pass

            direction = "above" if outcome.lower() in ("up", "yes") else "below"
            timeframe = _guess_timeframe(title, expiry_ts)

            market = PolymarketMarket(
                market_id     = md.get("id", condition_id),
                condition_id  = condition_id,
                coin          = coin,
                timeframe     = timeframe,
                strike        = 0.0,          # not required for copy-trade logic
                direction     = direction,
                yes_token_id  = yes_token_id,
                no_token_id   = no_token_id,
                yes_price     = price,
                expiry_timestamp = expiry_ts,
            )
            self._mkt_cache[condition_id] = market
            return market

        except Exception as e:
            logger.warning(f"CopyTrader _get_market error: {e}")
            return None


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _extract_coin(title: str) -> Optional[str]:
    lower = title.lower()
    for kw, coin in _COIN_MAP.items():
        if kw in lower:
            return coin
    return None


def _guess_timeframe(title: str, expiry_ts: float) -> str:
    lower = title.lower()
    if "1h" in lower or "1 hour" in lower:
        return "1h"
    tte = expiry_ts - time.time() if expiry_ts else 0
    # If market has more than 30 min remaining, treat as 1h; else 15m
    return "1h" if tte > 1800 else "15m"
