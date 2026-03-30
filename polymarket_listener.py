"""
polymarket_listener.py
Fetches and monitors Polymarket markets for configured crypto coins/timeframes.
Polls market list, YES prices, order book depth, and time to expiry.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import httpx

logger = logging.getLogger(__name__)


@dataclass
class OrderBookLevel:
    price: float
    size: float


@dataclass
class OrderBook:
    bids: List[OrderBookLevel] = field(default_factory=list)  # buy side
    asks: List[OrderBookLevel] = field(default_factory=list)  # sell side
    timestamp: float = 0.0

    def bid_depth_at_or_above(self, price: float) -> float:
        return sum(l.size for l in self.bids if l.price >= price)

    def ask_depth_at_or_below(self, price: float) -> float:
        return sum(l.size for l in self.asks if l.price <= price)

    def imbalance_ratio(self, price: float, depth: float = 0.02) -> float:
        """Bid/ask ratio near price. >1 means more buy pressure."""
        bid_vol = sum(l.size for l in self.bids if l.price >= price - depth)
        ask_vol = sum(l.size for l in self.asks if l.price <= price + depth)
        if ask_vol == 0:
            return 99.0
        return bid_vol / ask_vol

    def best_ask(self) -> Optional[float]:
        if not self.asks:
            return None
        return min(l.price for l in self.asks)

    def best_bid(self) -> Optional[float]:
        if not self.bids:
            return None
        return max(l.price for l in self.bids)


@dataclass
class PolymarketMarket:
    market_id: str
    condition_id: str
    question: str
    coin: str                   # BTC / ETH / SOL / XRP
    timeframe: str              # 5m / 15m
    strike: float               # the price level being bet on
    direction: str              # "above" or "below"
    yes_token_id: str
    no_token_id: str
    yes_price: float = 0.0
    expiry_timestamp: float = 0.0
    order_book: OrderBook = field(default_factory=OrderBook)
    last_updated: float = 0.0

    @property
    def seconds_to_expiry(self) -> float:
        return max(0.0, self.expiry_timestamp - time.time())

    @property
    def is_expired(self) -> bool:
        return self.seconds_to_expiry <= 0

    @property
    def is_sniper_window(self) -> bool:
        return self.seconds_to_expiry <= 40 and self.yes_price >= 0.99

    @property
    def is_standard_window(self) -> bool:
        return self.yes_price >= 0.98 and self.yes_price < 0.99


class PolymarketListener:
    """
    Polls Polymarket CLOB API for relevant crypto markets.
    Filters by coin, timeframe, and minimum price.
    """

    CLOB_BASE = "https://clob.polymarket.com"
    GAMMA_BASE = "https://gamma-api.polymarket.com"

    COIN_KEYWORDS = {
        "BTC": ["bitcoin", "btc"],
        "ETH": ["ethereum", "eth"],
        "SOL": ["solana", "sol"],
        "XRP": ["xrp", "ripple"],
    }

    TIMEFRAME_KEYWORDS = {
        "5m":  ["5-minute", "5 minute", "5min", "5m"],
        "15m": ["15-minute", "15 minute", "15min", "15m"],
    }

    def __init__(self, config: dict):
        self.config = config
        self.markets: Dict[str, PolymarketMarket] = {}
        self._running = False
        self._client: Optional[httpx.AsyncClient] = None
        self.poll_interval = 5.0

    async def start(self):
        self._running = True
        self._client = httpx.AsyncClient(timeout=10.0)
        asyncio.create_task(self._poll_loop())
        logger.info("PolymarketListener started")

    async def stop(self):
        self._running = False
        if self._client:
            await self._client.aclose()

    async def _poll_loop(self):
        while self._running:
            try:
                await self._refresh_markets()
                await self._refresh_prices()
                await self._refresh_order_books()
            except Exception as e:
                logger.warning(f"PolymarketListener poll error: {e}")
            await asyncio.sleep(self.poll_interval)

    async def _refresh_markets(self):
        """Fetch active markets and filter for configured coins/timeframes."""
        try:
            resp = await self._client.get(
                f"{self.GAMMA_BASE}/markets",
                params={"active": True, "closed": False, "limit": 500}
            )
            resp.raise_for_status()
            data = resp.json()
            markets_data = data if isinstance(data, list) else data.get("markets", [])

            for m in markets_data:
                parsed = self._parse_market(m)
                if parsed:
                    self.markets[parsed.market_id] = parsed
        except Exception as e:
            logger.warning(f"Failed to refresh market list: {e}")

    def _parse_market(self, m: dict) -> Optional[PolymarketMarket]:
        question = m.get("question", "").lower()
        description = m.get("description", "").lower()
        text = question + " " + description

        coin = self._detect_coin(text)
        if not coin:
            return None

        timeframe = self._detect_timeframe(text)
        if not timeframe:
            return None

        strike = self._extract_strike(text)
        if strike is None:
            return None

        direction = "above" if any(w in text for w in ["above", "over", "exceed", "higher"]) else "below"

        tokens = m.get("tokens", [m.get("clobTokenIds", [])])
        if isinstance(tokens, list) and len(tokens) >= 2:
            yes_token = tokens[0] if isinstance(tokens[0], str) else tokens[0].get("token_id", "")
            no_token = tokens[1] if isinstance(tokens[1], str) else tokens[1].get("token_id", "")
        else:
            return None

        expiry = m.get("endDateIso") or m.get("end_date_iso", "")
        expiry_ts = self._parse_expiry(expiry)
        if not expiry_ts or expiry_ts < time.time():
            return None

        return PolymarketMarket(
            market_id=str(m.get("id", m.get("conditionId", ""))),
            condition_id=str(m.get("conditionId", "")),
            question=m.get("question", ""),
            coin=coin,
            timeframe=timeframe,
            strike=strike,
            direction=direction,
            yes_token_id=yes_token,
            no_token_id=no_token,
            expiry_timestamp=expiry_ts,
            last_updated=time.time(),
        )

    def _detect_coin(self, text: str) -> Optional[str]:
        for coin, keywords in self.COIN_KEYWORDS.items():
            if any(kw in text for kw in keywords):
                return coin
        return None

    def _detect_timeframe(self, text: str) -> Optional[str]:
        for tf, keywords in self.TIMEFRAME_KEYWORDS.items():
            if any(kw in text for kw in keywords):
                return tf
        return None

    def _extract_strike(self, text: str) -> Optional[float]:
        import re
        # Match patterns like "$85,000", "85000", "$2,500"
        patterns = [
            r'\$?([\d,]+(?:\.\d+)?)\s*(?:usdt?|usd)?',
        ]
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                try:
                    val = float(match.replace(",", ""))
                    if val > 100:  # reasonable crypto price
                        return val
                except ValueError:
                    continue
        return None

    def _parse_expiry(self, expiry_str: str) -> Optional[float]:
        if not expiry_str:
            return None
        try:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
            return dt.timestamp()
        except Exception:
            return None

    async def _refresh_prices(self):
        """Fetch YES prices for all tracked markets."""
        if not self.markets:
            return
        token_ids = [m.yes_token_id for m in self.markets.values() if m.yes_token_id]
        if not token_ids:
            return
        try:
            resp = await self._client.get(
                f"{self.CLOB_BASE}/prices",
                params={"token_ids": ",".join(token_ids[:50])}
            )
            resp.raise_for_status()
            prices = resp.json()
            price_map = {str(p["token_id"]): float(p.get("price", 0)) for p in prices} \
                if isinstance(prices, list) else {}
            for market in self.markets.values():
                if market.yes_token_id in price_map:
                    market.yes_price = price_map[market.yes_token_id]
                    market.last_updated = time.time()
        except Exception as e:
            logger.debug(f"Price refresh error: {e}")

    async def _refresh_order_books(self):
        """Fetch order book for markets where YES price >= 0.97."""
        for market in list(self.markets.values()):
            if market.yes_price < 0.97:
                continue
            try:
                resp = await self._client.get(
                    f"{self.CLOB_BASE}/book",
                    params={"token_id": market.yes_token_id}
                )
                resp.raise_for_status()
                data = resp.json()
                bids = [
                    OrderBookLevel(price=float(b["price"]), size=float(b["size"]))
                    for b in data.get("bids", [])
                ]
                asks = [
                    OrderBookLevel(price=float(a["price"]), size=float(a["size"]))
                    for a in data.get("asks", [])
                ]
                market.order_book = OrderBook(
                    bids=bids, asks=asks, timestamp=time.time()
                )
            except Exception as e:
                logger.debug(f"Order book refresh error for {market.market_id}: {e}")
            await asyncio.sleep(0.1)  # gentle rate limit

    def get_active_markets(self) -> List[PolymarketMarket]:
        """Return all non-expired markets with YES price >= 0.98."""
        return [
            m for m in self.markets.values()
            if not m.is_expired and m.yes_price >= 0.98
        ]

    def get_market(self, market_id: str) -> Optional[PolymarketMarket]:
        return self.markets.get(market_id)
