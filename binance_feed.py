"""
binance_feed.py
Real-time Binance price feed via WebSocket.
Tracks price, momentum (1m/3m/5m), and volatility for configured symbols.
"""

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Optional
import websockets

logger = logging.getLogger(__name__)


@dataclass
class PricePoint:
    price: float
    timestamp: float  # unix epoch seconds


@dataclass
class SymbolData:
    symbol: str
    price: float = 0.0
    last_updated: float = 0.0
    # Rolling 5-minute price history (tick-level)
    history: deque = field(default_factory=lambda: deque(maxlen=3000))

    def record(self, price: float):
        self.price = price
        self.last_updated = time.time()
        self.history.append(PricePoint(price=price, timestamp=self.last_updated))

    def prices_last_n_seconds(self, n: int) -> list[float]:
        cutoff = time.time() - n
        return [p.price for p in self.history if p.timestamp >= cutoff]

    def momentum(self, window_seconds: int) -> Optional[float]:
        """Returns % price change over the last window_seconds. Positive = rising."""
        prices = self.prices_last_n_seconds(window_seconds)
        if len(prices) < 2:
            return None
        return (prices[-1] - prices[0]) / prices[0] * 100

    def volatility(self, window_seconds: int = 60) -> Optional[float]:
        """Rolling standard deviation of prices over window_seconds."""
        prices = self.prices_last_n_seconds(window_seconds)
        if len(prices) < 5:
            return None
        mean = sum(prices) / len(prices)
        variance = sum((p - mean) ** 2 for p in prices) / len(prices)
        return variance ** 0.5

    def is_stable(self, window_seconds: int = 30) -> bool:
        """True if price has not reversed direction in the last window_seconds."""
        prices = self.prices_last_n_seconds(window_seconds)
        if len(prices) < 5:
            return False
        direction = prices[-1] - prices[0]
        if direction == 0:
            return True
        # Check no opposing move > 50% of total move
        for i in range(1, len(prices)):
            move = prices[i] - prices[i - 1]
            if (direction > 0 and move < -(abs(direction) * 0.5)) or \
               (direction < 0 and move > (abs(direction) * 0.5)):
                return False
        return True

    def held_above(self, strike: float, window_seconds: int = 30) -> bool:
        """True if price has stayed above strike for the entire window."""
        prices = self.prices_last_n_seconds(window_seconds)
        if not prices:
            return False
        return all(p > strike for p in prices)

    def held_below(self, strike: float, window_seconds: int = 30) -> bool:
        """True if price has stayed below strike for the entire window."""
        prices = self.prices_last_n_seconds(window_seconds)
        if not prices:
            return False
        return all(p < strike for p in prices)


class BinanceFeed:
    """
    Maintains live WebSocket connections to Binance for configured symbols.
    Exposes per-symbol price, momentum, and volatility data.
    """

    # Binance US endpoint used for US-hosted servers (Railway, AWS us-*)
    # Falls back to global if US endpoint also fails
    BINANCE_WS_US   = "wss://stream.binance.us:9443/stream"
    BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"

    def __init__(self, symbols: Dict[str, str]):
        """
        symbols: mapping of coin name to Binance symbol, e.g. {"BTC": "BTCUSDT"}
        """
        self.symbols = symbols  # coin -> binance symbol
        self.data: Dict[str, SymbolData] = {
            coin: SymbolData(symbol=sym) for coin, sym in symbols.items()
        }
        self._running = False
        self._ws = None

    def get(self, coin: str) -> Optional[SymbolData]:
        return self.data.get(coin)

    def _stream_names(self) -> list[str]:
        return [f"{sym.lower()}@aggTrade" for sym in self.symbols.values()]

    async def start(self):
        self._running = True
        asyncio.create_task(self._run_forever())
        logger.info(f"BinanceFeed starting for symbols: {list(self.symbols.values())}")

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _run_forever(self):
        # Try Binance US first (for US-hosted servers), then global
        endpoints = [self.BINANCE_WS_US, self.BINANCE_WS_BASE]
        endpoint_idx = 0
        backoff = 1
        while self._running:
            try:
                await self._connect(endpoints[endpoint_idx % len(endpoints)])
                backoff = 1
            except Exception as e:
                logger.warning(
                    f"BinanceFeed error on {endpoints[endpoint_idx % len(endpoints)]}: {e}. "
                    f"Switching endpoint. Retrying in {backoff}s"
                )
                endpoint_idx += 1
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _connect(self, base_url: str):
        streams = "/".join(self._stream_names())
        url = f"{base_url}?streams={streams}"
        logger.info(f"BinanceFeed connecting to {base_url}")
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self._ws = ws
            logger.info("BinanceFeed connected")
            async for raw in ws:
                if not self._running:
                    break
                self._handle_message(raw)

    def _handle_message(self, raw: str):
        try:
            msg = json.loads(raw)
            data = msg.get("data", {})
            if data.get("e") != "aggTrade":
                return
            binance_symbol = data["s"]
            price = float(data["p"])
            coin = self._coin_for_symbol(binance_symbol)
            if coin:
                self.data[coin].record(price)
        except Exception as e:
            logger.debug(f"BinanceFeed parse error: {e}")

    def _coin_for_symbol(self, binance_symbol: str) -> Optional[str]:
        for coin, sym in self.symbols.items():
            if sym == binance_symbol:
                return coin
        return None

    def is_ready(self, coin: str) -> bool:
        """True if we have recent price data for this coin (within 5s)."""
        d = self.data.get(coin)
        if not d:
            return False
        return (time.time() - d.last_updated) < 5.0
