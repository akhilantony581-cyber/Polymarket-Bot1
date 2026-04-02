"""
main.py
Bot entry point. Wires all modules together and runs the main trading loop.
"""

import asyncio
import logging
import os
import signal
import sys
import time
from pathlib import Path
import yaml

from binance_feed import BinanceFeed
from polymarket_listener import PolymarketListener
from signal_engine import SignalEngine, TradeMode
from execution_engine import ExecutionEngine
from order_manager import OrderManager
from risk_manager import RiskManager
from structured_logger import StructuredLogger

logger = logging.getLogger(__name__)


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)
    # Expand environment variables in string values
    def expand(obj):
        if isinstance(obj, str):
            return os.path.expandvars(obj)
        if isinstance(obj, dict):
            return {k: expand(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [expand(i) for i in obj]
        return obj
    return expand(raw)


def setup_logging(config: dict):
    log_cfg = config.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("logs/bot.log"),
        ],
    )


class TradingBot:
    def __init__(self, config: dict):
        self.config = config
        self._running = False
        self._config_path = "config.yaml"
        self._last_config_mtime = 0.0

        # Initialize all modules
        coins = config["markets"]["coins"]
        binance_symbols = config["markets"]["binance_symbols"]
        symbols = {c: binance_symbols[c] for c in coins if c in binance_symbols}

        self.binance = BinanceFeed(symbols=symbols)
        self.poly_listener = PolymarketListener(config=config)
        self.signal_engine = SignalEngine(config=config, binance_feed=self.binance)
        self.execution = ExecutionEngine(config=config)
        self.risk_manager = RiskManager(config=config)
        self.structured_log = StructuredLogger(config=config)

        self.order_manager = OrderManager(
            config=config,
            execution=self.execution,
            signal_engine=self.signal_engine,
            poly_listener=self.poly_listener,
            on_fill=self._on_fill,
            on_cancel=self._on_cancel,
            on_redeem=self._on_redeem,
        )

    async def start(self):
        self._running = True
        logger.info("=" * 60)
        logger.info("Polymarket Trading Bot starting")
        logger.info(f"Capital: ${self.config['capital']['total']}")
        logger.info(f"Min entry price: {self.config['price']['min_entry']}")
        logger.info(f"Coins: {self.config['markets']['coins']}")
        logger.info("=" * 60)

        # Start all data feeds
        await self.binance.start()
        await self.poly_listener.start()
        await self.order_manager.start()

        # Give feeds time to warm up
        logger.info("Waiting for data feeds to warm up (5s)...")
        await asyncio.sleep(5)

        # Main loop
        await asyncio.gather(
            self._trading_loop(),
            self._config_watcher(),
        )

    async def stop(self):
        self._running = False
        await self.binance.stop()
        await self.poly_listener.stop()
        await self.order_manager.stop()
        await self.execution.close()
        await self.risk_manager.close()
        logger.info("Bot stopped cleanly")

    # ------------------------------------------------------------------
    # MAIN TRADING LOOP
    # ------------------------------------------------------------------
    async def _trading_loop(self):
        _diag_tick = 0
        while self._running:
            try:
                await self._scan_markets()
                _diag_tick += 1
                if _diag_tick % 60 == 0:  # Log diagnostics every ~60 seconds
                    await self._log_diagnostics()
            except Exception as e:
                logger.error(f"Trading loop error: {e}", exc_info=True)
            await asyncio.sleep(1.0)

    async def _log_diagnostics(self):
        """Log top market prices and bot state every 60s to help diagnose missed trades."""
        markets = list(self.poly_listener.markets.values())
        if not markets:
            logger.warning("DIAG: No markets in listener — listener may not be fetching data")
            return

        # Find the highest-priced token across all markets
        top = sorted(
            [(m.coin, m.timeframe, *m.best_trade_side, round(m.seconds_to_expiry, 0))
             for m in markets if not m.is_expired],
            key=lambda x: -x[3]  # sort by price descending (index 3 = price)
        )
        # top[i] = (coin, timeframe, side, price, tte)
        top5 = [(f"{c} {tf} {side.upper()}={price:.4f} tte={tte:.0f}s")
                for c, tf, side, price, tte in top[:5]]

        halted = self.risk_manager.is_halted
        paused = self.risk_manager.is_paused
        active = self.order_manager.active_count
        can, reason = self.risk_manager.can_trade(active)

        logger.info(
            f"DIAG: {len(markets)} markets tracked | "
            f"active_orders={active} | halted={halted} paused={paused} | "
            f"can_trade={can} ({reason}) | "
            f"top prices: {', '.join(top5) if top5 else 'none'}"
        )

    async def _scan_markets(self):
        sniper_size = self.config["capital"].get("max_per_trade", 20.0)
        max_per_market = self.config["capital"].get("max_per_market", 40.0)

        qualifying = []
        for market in list(self.poly_listener.markets.values()):
            if market.is_expired:
                continue

            # Only trade within 90 seconds of expiry
            if market.seconds_to_expiry > 90:
                continue

            side, price = market.best_trade_side

            # For markets approaching threshold within final 2 minutes,
            # fetch a fresh CLOB price right now rather than relying on cache.
            if 0.90 <= price < 0.99 and market.seconds_to_expiry <= 300:
                await self.poly_listener._fetch_clob_prices_for_market(market)
                side, price = market.best_trade_side

            price = min(price, 0.99)  # CLOB max price is 0.99
            if price < 0.99:
                continue

            if self._market_has_active_order(market.market_id):
                continue

            # Enforce max per market cap
            market_exposure = self._market_exposure(market.market_id)
            if market_exposure >= max_per_market:
                continue

            can, reason = self.risk_manager.can_trade(self.order_manager.active_count + len(qualifying))
            if not can:
                break

            qualifying.append((market, side, price))

        if not qualifying:
            return

        # Submit all qualifying orders in parallel — don't let one network
        # call block the others while the 0.99 window closes.
        async def _submit_one(market, side, price):
            logger.info(
                f"SNIPER [{market.coin} {market.timeframe}] "
                f"{side.upper()}@{price:.4f} size=${sniper_size:.2f} tte={market.seconds_to_expiry:.0f}s"
            )
            pos = await self.order_manager.submit(
                market=market,
                price=price,
                usdc_size=sniper_size,
                mode="sniper",
            )
            if pos:
                self.risk_manager.record_trade_open(pos)

        await asyncio.gather(*[_submit_one(m, s, p) for m, s, p in qualifying])

    async def _execute_trade(self, market, signal, size: float):
        mode = signal.mode.value
        price = self._entry_price(market, signal)

        side, _ = market.best_trade_side
        logger.info(
            f"Executing [{mode}] {market.coin} {market.timeframe} "
            f"{side.upper()}@{price:.4f} size=${size:.2f} score={signal.reversal_score:.1f}"
        )

        pos = await self.order_manager.submit(
            market=market,
            price=price,
            usdc_size=size,
            mode=mode,
        )
        if pos:
            self.risk_manager.record_trade_open(pos)

    async def _scan_maker_opportunities(self, markets):
        can, _ = self.risk_manager.can_trade(self.order_manager.active_count)
        if not can:
            return

        for market in markets:
            if self._market_has_active_order(market.market_id):
                continue
            maker_signal = self.signal_engine.evaluate_maker(market)
            if not maker_signal:
                continue

            size = self.risk_manager.position_size(
                maker_signal, self.order_manager.active_count
            )
            if size <= 0:
                continue

            maker_cfg = self.config.get("maker", {})
            lo, hi = maker_cfg.get("post_price_range", [0.93, 0.96])
            maker_price = round(lo + (hi - lo) * 0.5, 4)  # post at midpoint

            await self._execute_trade(market, maker_signal, size)

    def _entry_price(self, market, signal) -> float:
        """Determine limit price based on mode."""
        if signal.mode == TradeMode.SNIPER:
            # Aggressive: near best ask
            best_ask = market.order_book.best_ask()
            if best_ask and best_ask >= self.config["price"]["min_entry"]:
                return best_ask
            return market.yes_price

        elif signal.mode == TradeMode.STANDARD:
            # Passive: slight discount to best ask
            offset = self.config.get("standard", {}).get("passive_limit_offset", 0.001)
            best_ask = market.order_book.best_ask()
            if best_ask:
                price = best_ask - offset
                return max(price, self.config["price"]["min_entry"])
            return market.yes_price

        elif signal.mode == TradeMode.MAKER:
            lo, hi = self.config.get("maker", {}).get("post_price_range", [0.93, 0.96])
            return round(lo + (hi - lo) * 0.5, 4)

        return market.yes_price

    def _market_exposure(self, market_id: str) -> float:
        """Total USDC committed to a market across active orders and filled positions."""
        total = sum(
            p.entry_usdc for p in self.order_manager.active_orders.values()
            if p.market.market_id == market_id
        )
        total += sum(
            p.entry_usdc for p in self.order_manager.filled_positions.values()
            if p.market.market_id == market_id and not p.redeemed
        )
        return total

    def _market_has_active_order(self, market_id: str) -> bool:
        for pos in self.order_manager.active_orders.values():
            if pos.market.market_id == market_id:
                return True
        return False

    # ------------------------------------------------------------------
    # CALLBACKS
    # ------------------------------------------------------------------
    def _on_fill(self, pos):
        logger.info(f"FILL: {pos.market.market_id} @ {pos.entry_price:.4f} ${pos.entry_usdc:.2f}")
        self.structured_log.log_trade_open(pos)

    def _on_cancel(self, pos, reason: str):
        self.risk_manager.record_cancelled(pos)
        self.structured_log.log_cancel(pos, reason)

    def _on_redeem(self, pos):
        self.risk_manager.record_trade_closed(pos, reason="redeemed")
        self.structured_log.log_trade_closed(pos)

    # ------------------------------------------------------------------
    # CONFIG HOT-RELOAD WATCHER
    # ------------------------------------------------------------------
    async def _config_watcher(self):
        interval = self.config.get("dashboard", {}).get("config_watch_interval", 2.0)
        while self._running:
            await asyncio.sleep(interval)
            try:
                mtime = Path(self._config_path).stat().st_mtime
                if mtime > self._last_config_mtime:
                    self._last_config_mtime = mtime
                    new_config = load_config(self._config_path)
                    self.config = new_config
                    self.signal_engine.reload_config(new_config)
                    self.risk_manager.reload_config(new_config)
                    logger.info("Config reloaded from disk")
            except Exception as e:
                logger.warning(f"Config reload error: {e}")

    # ------------------------------------------------------------------
    # EXPOSE STATE FOR DASHBOARD
    # ------------------------------------------------------------------
    def get_state(self) -> dict:
        return {
            "running": self._running,
            "paused": self.risk_manager.is_paused,
            "halted": self.risk_manager.is_halted,
            "metrics": self.risk_manager.metrics.to_dict(),
            "active_orders": [
                {
                    "order_id": p.order.order_id,
                    "market_id": p.market.market_id,
                    "coin": p.market.coin,
                    "timeframe": p.market.timeframe,
                    "mode": p.mode,
                    "price": p.entry_price,
                    "size": p.entry_usdc,
                    "age": round(p.order.age_seconds, 1),
                }
                for p in self.order_manager.active_orders.values()
            ],
            "positions": [
                {
                    "order_id": p.order.order_id,
                    "market_id": p.market.market_id,
                    "coin": p.market.coin,
                    "mode": p.mode,
                    "entry_price": p.entry_price,
                    "size": p.entry_usdc,
                    "redeemed": p.redeemed,
                    "pnl": p.pnl,
                }
                for p in self.order_manager.filled_positions.values()
            ],
            "recent_trades": [
                t.to_dict() for t in self.risk_manager.trade_history[-20:]
            ],
            "config": {
                "min_entry": self.config["price"]["min_entry"],
                "total_capital": self.config["capital"]["total"],
                "max_concurrent": self.config["capital"]["max_concurrent_trades"],
                "max_per_trade": self.config["capital"].get("max_per_trade", 10.0),
                "max_per_market": self.config["capital"].get("max_per_market", 20.0),
                "min_trading_price": self.config["price"].get("min_entry", 0.99),
            },
            "prices": self._get_prices(),
        }

    def _get_prices(self) -> dict:
        result = {}
        for coin in ["BTC", "ETH", "SOL", "XRP"]:
            bd = self.binance.get(coin)
            # Find Polymarket Up market for this coin (any timeframe)
            up_market = next(
                (m for m in self.poly_listener.markets.values()
                 if m.coin == coin and not m.is_expired),
                None
            )
            result[coin] = {
                "binance": round(bd.price, 2) if bd else None,
                "up_ask": round(up_market.yes_price, 4) if up_market else None,
                "down_ask": round(1 - up_market.yes_price, 4) if up_market else None,
                "momentum_1m": round(bd.momentum(60), 3) if bd and bd.momentum(60) is not None else None,
            }
        return result


# ------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------
async def main():
    Path("logs").mkdir(exist_ok=True)
    config = load_config("config.yaml")
    setup_logging(config)

    bot = TradingBot(config)

    # Make bot accessible to dashboard routes (same process)
    import builtins
    builtins._bot = bot

    # Start dashboard server in background within same process
    import uvicorn
    from dashboard import app as dashboard_app
    dash_cfg = config.get("dashboard", {})
    server_config = uvicorn.Config(
        dashboard_app,
        host=dash_cfg.get("host", "0.0.0.0"),
        port=dash_cfg.get("port", 8080),
        log_level="warning",
    )
    server = uvicorn.Server(server_config)
    asyncio.create_task(server.serve())

    # Hook Python logging into the dashboard WebSocket live log
    from dashboard import install_log_handler
    install_log_handler()

    # Graceful shutdown
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(bot.stop()))

    try:
        await bot.start()
    except asyncio.CancelledError:
        pass
    finally:
        await bot.stop()


if __name__ == "__main__":
    asyncio.run(main())
