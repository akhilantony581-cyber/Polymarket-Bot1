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
    def no_price(self) -> float:
        """Price of the DOWN token (complement of UP token)."""
        return round(1.0 - self.yes_price, 4)

    @property
    def best_trade_side(self) -> tuple:
        """Returns ('yes', price) or ('no', price) — whichever token is winning."""
        if self.no_price > self.yes_price:
            return ('no', self.no_price)
        return ('yes', self.yes_price)

    @property
    def trade_token_id(self) -> str:
        """Token ID to buy based on which side is winning."""
        side, _ = self.best_trade_side
        return self.no_token_id if side == 'no' else self.yes_token_id

    @property
    def trade_price(self) -> float:
        """Price of the token we'd buy."""
        _, price = self.best_trade_side
        return price

    @property
    def is_sniper_window(self) -> bool:
        _, price = self.best_trade_side
        return price >= 0.98

    @property
    def is_standard_window(self) -> bool:
        _, price = self.best_trade_side
        return 0.94 <= price < 0.95


class PolymarketListener:
    """
    Polls Polymarket CLOB API for relevant crypto markets.
    Filters by coin, timeframe, and minimum price.
    """

    CLOB_BASE = "https://clob.polymarket.com"
    GAMMA_BASE = "https://gamma-api.polymarket.com"

    # Slug prefixes used in Polymarket recurring series
    COIN_SLUGS = {
        "BTC": "btc",
        "ETH": "eth",
        "SOL": "sol",
        "XRP": "xrp",
    }

    TIMEFRAME_SECONDS = {
        "5m":  300,
        "15m": 900,
    }

    # Keep for legacy parsing fallback
    COIN_KEYWORDS = {
        "BTC": ["bitcoin", "btc"],
        "ETH": ["ethereum", "eth"],
        "SOL": ["solana", "sol"],
        "XRP": ["xrp", "ripple"],
    }

    TIMEFRAME_KEYWORDS = {
        "5m":  ["5-minute", "5 minute", "5min", "5m", "5 min", "300s", "5-min"],
        "15m": ["15-minute", "15 minute", "15min", "15m", "15 min", "900s", "15-min"],
    }

    def __init__(self, config: dict):
        self.config = config
        self.markets: Dict[str, PolymarketMarket] = {}
        self._running = False
        self._client: Optional[httpx.AsyncClient] = None
        self.poll_interval = 2.0

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
                await self._refresh_markets()   # prices included via bestAsk
                await self._refresh_order_books()
            except Exception as e:
                logger.warning(f"PolymarketListener poll error: {e}")
            await asyncio.sleep(self.poll_interval)

    def _current_epoch(self, tf_seconds: int) -> int:
        """Return the current epoch aligned to the timeframe boundary."""
        return (int(time.time()) // tf_seconds) * tf_seconds

    async def _refresh_markets(self):
        """
        Fetch crypto 5m/15m markets by constructing their deterministic slugs.
        Polymarket recurring markets follow: {coin}-updown-{tf}-{epoch}
        where epoch is the Unix timestamp of the window start, aligned to
        the timeframe (5m=300s, 15m=900s).
        We fetch current + next window for each coin/timeframe pair.
        """
        import json as _json
        found = 0
        errors = 0

        for coin, slug_prefix in self.COIN_SLUGS.items():
            for tf, tf_sec in self.TIMEFRAME_SECONDS.items():
                current_epoch = self._current_epoch(tf_sec)
                # Fetch current window and next window (in case current just closed)
                for epoch in [current_epoch, current_epoch + tf_sec]:
                    slug = f"{slug_prefix}-updown-{tf}-{epoch}"
                    try:
                        resp = await self._client.get(
                            f"{self.GAMMA_BASE}/events/slug/{slug}"
                        )
                        if resp.status_code == 404:
                            continue
                        if resp.status_code != 200:
                            logger.debug(f"Slug {slug}: HTTP {resp.status_code}")
                            continue

                        event = resp.json()

                        # Skip closed events
                        if event.get("closed", False):
                            continue

                        # Extract nested market (contains token IDs and prices)
                        markets_list = event.get("markets", [])
                        if not markets_list:
                            logger.debug(f"Slug {slug}: no nested markets")
                            continue

                        m = markets_list[0]

                        # Parse clobTokenIds — it's a JSON string in the API response
                        raw_token_ids = m.get("clobTokenIds", "[]")
                        if isinstance(raw_token_ids, str):
                            token_ids = _json.loads(raw_token_ids)
                        else:
                            token_ids = raw_token_ids

                        if len(token_ids) < 2:
                            logger.debug(f"Slug {slug}: missing token IDs")
                            continue

                        yes_token = str(token_ids[0])  # "Up" token
                        no_token  = str(token_ids[1])  # "Down" token

                        # Expiry
                        expiry_str = m.get("endDate") or event.get("endDate") or ""
                        expiry_ts = self._parse_expiry(expiry_str)
                        if not expiry_ts or expiry_ts < time.time():
                            continue

                        # Price from market object (bestAsk is the YES ask price)
                        yes_price = float(m.get("bestAsk") or m.get("lastTradePrice") or 0)

                        market_id = str(m.get("id") or m.get("conditionId", slug))
                        condition_id = str(m.get("conditionId", ""))

                        pm = PolymarketMarket(
                            market_id=market_id,
                            condition_id=condition_id,
                            question=m.get("question") or event.get("title", slug),
                            coin=coin,
                            timeframe=tf,
                            strike=0.0,       # Up/Down markets have no fixed strike
                            direction="up",   # we always trade the "Up" token
                            yes_token_id=yes_token,
                            no_token_id=no_token,
                            yes_price=yes_price,
                            expiry_timestamp=expiry_ts,
                            last_updated=time.time(),
                        )
                        self.markets[market_id] = pm
                        found += 1
                        logger.debug(f"Tracked: {slug} price={yes_price:.3f} tte={pm.seconds_to_expiry:.0f}s")

                    except Exception as e:
                        errors += 1
                        logger.debug(f"Slug {slug} error: {e}")

                    await asyncio.sleep(0.05)

        logger.info(f"Market refresh: {found} markets tracked ({errors} errors)")

        # Prune expired markets so they don't accumulate across cycles
        expired_ids = [mid for mid, m in self.markets.items() if m.is_expired]
        for mid in expired_ids:
            del self.markets[mid]
        if expired_ids:
            logger.debug(f"Pruned {len(expired_ids)} expired markets")

    def is_valid_market(self, m: dict) -> bool:
        """Quick pre-filter: crypto keyword + timeframe + not closed."""
        if m.get("closed", False):
            return False
        title = (m.get("question") or m.get("title") or "").lower()
        has_coin = any(
            kw in title
            for keywords in self.COIN_KEYWORDS.values()
            for kw in keywords
        )
        if not has_coin:
            return False
        has_tf = any(
            kw in title
            for keywords in self.TIMEFRAME_KEYWORDS.values()
            for kw in keywords
        )
        return has_tf

    def _parse_market(self, m: dict) -> Optional[PolymarketMarket]:
        # Use question OR title (Gamma API uses both field names)
        question = (m.get("question") or m.get("title") or "").lower()
        description = m.get("description", "").lower()
        text = question + " " + description

        coin = self._detect_coin(text)
        if not coin:
            return None

        timeframe = self._detect_timeframe(text)
        if not timeframe:
            return None

        # Skip closed markets
        if m.get("closed", False):
            return None

        # "Up or Down" directional markets — no fixed strike
        is_up_down = "up or down" in text or "up/down" in text
        if is_up_down:
            strike = 0.0
            direction = "up"
        else:
            strike = self._extract_strike(text)
            if strike is None:
                return None
            direction = "above" if any(
                w in text for w in ["above", "over", "exceed", "higher"]
            ) else "below"

        # Token extraction — clobTokenIds can be a JSON string or list
        import json as _json
        raw = m.get("clobTokenIds") or m.get("tokens") or "[]"
        if isinstance(raw, str):
            try:
                tokens = _json.loads(raw)
            except Exception:
                tokens = []
        else:
            tokens = raw
        yes_token = ""
        no_token = ""
        if len(tokens) >= 2:
            t0, t1 = tokens[0], tokens[1]
            yes_token = t0 if isinstance(t0, str) else t0.get("token_id", "")
            no_token  = t1 if isinstance(t1, str) else t1.get("token_id", "")

        # Expiry field — try all known field names
        expiry = (m.get("endDate") or m.get("endDateIso") or
                  m.get("end_date_iso") or m.get("end_date") or "")
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
        """Fetch YES prices in batches of 10 to avoid URL length limits."""
        if not self.markets:
            return
        markets_with_tokens = [m for m in self.markets.values() if m.yes_token_id]
        if not markets_with_tokens:
            return

        # Batch into groups of 10 to keep URLs short
        BATCH = 10
        for i in range(0, len(markets_with_tokens), BATCH):
            batch = markets_with_tokens[i:i + BATCH]
            token_ids = [m.yes_token_id for m in batch]
            try:
                resp = await self._client.get(
                    f"{self.CLOB_BASE}/prices",
                    params={"token_ids": ",".join(token_ids)}
                )
                if resp.status_code != 200:
                    logger.debug(f"Price refresh HTTP {resp.status_code}")
                    continue
                prices = resp.json()
                price_map = {str(p["token_id"]): float(p.get("price", 0)) for p in prices} \
                    if isinstance(prices, list) else {}
                for market in batch:
                    if market.yes_token_id in price_map:
                        market.yes_price = price_map[market.yes_token_id]
                        market.last_updated = time.time()
            except Exception as e:
                logger.debug(f"Price refresh batch error: {e}")
            await asyncio.sleep(0.1)

    async def _refresh_order_books(self):
        """
        For markets within 3 minutes of expiry: fetch live CLOB mid-price
        to replace the lagged Gamma bestAsk. Also fetch order book depth.
        For all others: fetch order book only if price >= 0.97.
        """
        for market in list(self.markets.values()):
            tte = market.seconds_to_expiry
            near_expiry = tte <= 180  # within 3 minutes

            # For near-expiry markets: fetch live CLOB price for both tokens
            if near_expiry and market.yes_token_id and market.no_token_id:
                try:
                    # Fetch mid-price for YES token
                    resp = await self._client.get(
                        f"{self.CLOB_BASE}/midpoint",
                        params={"token_id": market.yes_token_id}
                    )
                    if resp.status_code == 200:
                        mid = float(resp.json().get("mid", 0))
                        if mid > 0:
                            market.yes_price = mid
                            market.last_updated = time.time()
                            logger.debug(f"CLOB mid-price {market.coin} {market.timeframe}: {mid:.4f} tte={tte:.0f}s")
                except Exception as e:
                    logger.debug(f"CLOB price refresh error: {e}")
                await asyncio.sleep(0.05)

            # Fetch order book for high-price markets
            if market.yes_price < 0.95 and not near_expiry:
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
            await asyncio.sleep(0.05)

    def get_active_markets(self) -> List[PolymarketMarket]:
        """Return non-expired markets where best side (UP or DOWN) >= 0.94.
        Checks both tokens — doubles opportunity detection."""
        result = []
        for m in self.markets.values():
            if m.is_expired:
                continue
            _, price = m.best_trade_side
            if 0.99 <= price < 1.0:
                result.append(m)
        return result

    def get_market(self, market_id: str) -> Optional[PolymarketMarket]:
        return self.markets.get(market_id)
