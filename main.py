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
from order_manager import OrderManager, ManagedPosition
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

        # Main loop — all top-level coroutines are supervised (auto-restart on crash)
        await asyncio.gather(
            self._supervise(self._trading_loop, "trading_loop"),
            self._supervise(self._config_watcher, "config_watcher"),
            self._supervise(self._keepalive_loop, "keepalive_loop"),
            self._supervise(self._watchdog_loop, "watchdog_loop"),
            self._supervise(self._pulse_loop, "pulse_loop"),
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
    # SUPERVISOR — restarts any loop that crashes
    # ------------------------------------------------------------------
    async def _supervise(self, coro_func, name: str):
        while self._running:
            try:
                await coro_func()
            except Exception as e:
                logger.error(f"[WATCHDOG] {name} crashed: {e} — restarting in 3s", exc_info=True)
                await asyncio.sleep(3)

    # ------------------------------------------------------------------
    # PULSE — toggles max_per_trade base→base+1→base every 5 min to stay active
    # ------------------------------------------------------------------
    async def _pulse_loop(self):
        base = self.config["capital"].get("max_per_trade", 10.0)
        while self._running:
            self.config["capital"]["max_per_trade"] = base + 1
            await asyncio.sleep(300)
            self.config["capital"]["max_per_trade"] = base
            await asyncio.sleep(300)

    # ------------------------------------------------------------------
    # WATCHDOG — detects stalls and auto-recovers
    # ------------------------------------------------------------------
    async def _watchdog_loop(self):
        while self._running:
            await asyncio.sleep(60)

            # Auto-reset halt so bot never stays stopped permanently
            if self.risk_manager.is_halted:
                logger.warning("[WATCHDOG] Bot was halted — auto-resetting to keep trading")
                self.risk_manager.reset_halt()

            markets = list(self.poly_listener.markets.values())
            if not markets:
                logger.warning("[WATCHDOG] 0 markets tracked — listener may be stalled or proxy down")
            if not self.order_manager._running:
                logger.error("[WATCHDOG] OrderManager stopped — restarting")
                await self.order_manager.start()

    # ------------------------------------------------------------------
    # MAIN TRADING LOOP
    # ------------------------------------------------------------------
    async def _trading_loop(self):
        _diag_tick = 0
        while self._running:
            try:
                await self._scan_markets()
                await self._scan_maker_both_sides()
                _diag_tick += 1
                if _diag_tick % 30 == 0:
                    await self._log_diagnostics()
            except Exception as e:
                logger.error(f"Trading loop error: {e}", exc_info=True)
            await asyncio.sleep(1.0)

    async def _log_diagnostics(self):
        """Log market state every 30s — shows exactly why bot is/isn't trading."""
        markets = list(self.poly_listener.markets.values())
        if not markets:
            logger.warning("DIAG: 0 markets tracked — proxy down or listener error")
            return

        active = self.order_manager.active_count
        can, reason = self.risk_manager.can_trade(active)
        halted = self.risk_manager.is_halted
        paused = self.risk_manager.is_paused

        # Check Binance feed health
        binance_ok = [c for c in self.config["markets"]["coins"] if self.binance.is_ready(c)]
        binance_stale = [c for c in self.config["markets"]["coins"] if not self.binance.is_ready(c)]

        # Find best candidates and why they're blocked
        sniper_min = self.config.get("price", {}).get("sniper_min", 0.97)
        near_expiry = [m for m in markets if not m.is_expired and m.seconds_to_expiry <= 600]
        qualifying_price = [m for m in near_expiry if m.best_trade_side[1] >= sniper_min]

        top = sorted(
            [(m.coin, m.timeframe, *m.best_trade_side, round(m.seconds_to_expiry, 0))
             for m in markets if not m.is_expired],
            key=lambda x: -x[3]
        )
        top5 = [f"{c} {tf} {s.upper()}={p:.2f} tte={t:.0f}s" for c, tf, s, p, t in top[:5]]

        logger.info(
            f"DIAG | markets={len(markets)} near_expiry={len(near_expiry)} "
            f"price_ok={len(qualifying_price)} active={active} "
            f"can_trade={can}({reason}) halted={halted} paused={paused} | "
            f"binance_ok={binance_ok} stale={binance_stale} | "
            f"top: {', '.join(top5) if top5 else 'none'}"
        )

    async def _scan_markets(self):
        max_per_trade  = self.config["capital"].get("max_per_trade", 20.0)
        max_per_market = self.config["capital"].get("max_per_market", 40.0)

        # Snipe 1 threshold (default 0.97 — lower than old 0.99 for more trades)
        sniper_min = self.config.get("price", {}).get("sniper_min", 0.97)

        # Momentum gate config
        mg = self.config.get("momentum_gate", {})
        mg_enabled  = mg.get("enabled", True)
        mg_window   = mg.get("window_seconds", 30)
        mg_min_pct  = mg.get("min_pct", -0.05)    # allow up to -0.05% drift against direction
        mg_boundary = mg.get("boundary_pct", 0.02) # skip if |momentum| < 0.02% (undecided)

        qualifying = []
        for market in list(self.poly_listener.markets.values()):
            if market.is_expired:
                continue

            # trade windows: 1h=600s, 15m=150s, 5m=150s
            tf_windows = {"1h": 600, "15m": 150, "5m": 150}
            window = tf_windows.get(market.timeframe, 150)
            if market.seconds_to_expiry > window:
                continue

            side, price = market.best_trade_side

            # Fresh CLOB fetch when approaching threshold
            if price < (sniper_min + 0.05) and market.seconds_to_expiry <= window:
                await self.poly_listener._fetch_clob_prices_for_market(market)
                side, price = market.best_trade_side

            price = min(price, 0.99)
            if price < sniper_min:
                continue

            # ── Momentum gate (suggestions 2 & 4) ──────────────────────────
            if mg_enabled:
                bd = self.binance.get(market.coin)
                binance_ready = bd and self.binance.is_ready(market.coin)

                if not binance_ready:
                    # No Binance data for this coin (e.g. HYPE not on Binance).
                    # Fall back to strict threshold — only take near-certain outcomes.
                    strict = min(0.97, sniper_min + 0.05)
                    if price < strict:
                        logger.debug(
                            f"No Binance data for {market.coin} — "
                            f"requiring {strict:.2f}, got {price:.4f}, skipping"
                        )
                        continue
                else:
                    mom = bd.momentum(mg_window)
                    if mom is not None:
                        buying_up = (side == "yes")
                        # Require momentum to be actively in our favour (not just "not bad")
                        if buying_up and mom < mg_min_pct:
                            logger.debug(
                                f"Momentum gate SKIP {market.coin} {market.timeframe} "
                                f"UP blocked mom={mom:.3f}%"
                            )
                            continue
                        if not buying_up and mom > -mg_min_pct:
                            logger.debug(
                                f"Momentum gate SKIP {market.coin} {market.timeframe} "
                                f"DOWN blocked mom={mom:.3f}%"
                            )
                            continue
                        # Near-boundary skip: momentum too weak to confirm direction
                        if market.seconds_to_expiry > 5 and abs(mom) < mg_boundary:
                            logger.debug(
                                f"Boundary skip {market.coin} {market.timeframe} "
                                f"mom={mom:.3f}% < boundary threshold"
                            )
                            continue

            if self._market_has_active_order(market.market_id):
                continue

            market_exposure = self._market_exposure(market.market_id)
            if market_exposure >= max_per_market:
                continue

            can, reason = self.risk_manager.can_trade(self.order_manager.active_count + len(qualifying))
            if not can:
                break

            # Kelly-scaled size: larger bet the higher the probability
            kelly_size = self._kelly_size(price, sniper_min, max_per_trade)
            qualifying.append((market, side, price, kelly_size))

        if not qualifying:
            return

        async def _submit_one(market, side, price, size):
            logger.info(
                f"SNIPER [{market.coin} {market.timeframe}] "
                f"{side.upper()}@{price:.4f} size=${size:.2f} tte={market.seconds_to_expiry:.0f}s"
            )
            pos = await self.order_manager.submit(
                market=market,
                price=price,
                usdc_size=size,
                mode="sniper",
            )
            if pos:
                self.risk_manager.record_trade_open(pos)

        await asyncio.gather(*[_submit_one(m, s, p, sz) for m, s, p, sz in qualifying])

        # ── Snipe 2: last 10 seconds, price >= 0.95 ────────────────────────
        s2 = self.config.get("snipe2", {})
        if not s2.get("enabled", True):
            return
        s2_min_price  = s2.get("min_price", 0.95)
        s2_max_trade  = s2.get("max_per_trade", 10.0)
        s2_max_market = s2.get("max_per_market", 20.0)
        s2_window     = s2.get("window_seconds", 10)

        s2_qualifying = []
        for market in list(self.poly_listener.markets.values()):
            if market.is_expired:
                continue
            if market.seconds_to_expiry > s2_window:
                continue
            side, price = market.best_trade_side
            price = min(price, 0.99)
            if price < s2_min_price:
                continue

            # Momentum gate for snipe2
            if mg_enabled:
                bd = self.binance.get(market.coin)
                binance_ready = bd and self.binance.is_ready(market.coin)
                if not binance_ready:
                    # No Binance data — require stricter price floor for snipe2 too
                    s2_strict = min(0.95, s2_min_price + 0.04)
                    if price < s2_strict:
                        logger.debug(f"S2 no Binance data for {market.coin}, price {price:.4f} < {s2_strict:.2f}, skipping")
                        continue
                else:
                    mom = bd.momentum(15)  # shorter window for last-10s trades
                    if mom is not None:
                        buying_up = (side == "yes")
                        hard_block = mg_min_pct * 3  # only block truly sharp reversals
                        if buying_up and mom < hard_block:
                            logger.debug(f"S2 momentum gate SKIP {market.coin} UP mom={mom:.3f}%")
                            continue
                        if not buying_up and mom > -hard_block:
                            logger.debug(f"S2 momentum gate SKIP {market.coin} DOWN mom={mom:.3f}%")
                            continue

            if self._market_has_active_order(market.market_id):
                continue
            if self._market_exposure(market.market_id) >= s2_max_market:
                continue
            can, _ = self.risk_manager.can_trade(self.order_manager.active_count + len(s2_qualifying))
            if not can:
                break

            kelly_size = self._kelly_size(price, s2_min_price, s2_max_trade)
            s2_qualifying.append((market, side, price, kelly_size))

        async def _submit_snipe2(market, side, price, size):
            logger.info(
                f"SNIPE2 [{market.coin} {market.timeframe}] "
                f"{side.upper()}@{price:.4f} size=${size:.2f} tte={market.seconds_to_expiry:.0f}s"
            )
            pos = await self.order_manager.submit(
                market=market, price=price, usdc_size=size, mode="snipe2",
            )
            if pos:
                self.risk_manager.record_trade_open(pos)

        if s2_qualifying:
            await asyncio.gather(*[_submit_snipe2(m, s, p, sz) for m, s, p, sz in s2_qualifying])

    # ------------------------------------------------------------------
    # MARKET MAKER — both sides, 10 contracts, price 0.90–0.97
    # ------------------------------------------------------------------
    async def _scan_maker_both_sides(self):
        mm = self.config.get("market_maker", {})
        if not mm.get("enabled", True):
            return

        mm_min     = mm.get("min_price", 0.90)    # winning side must be >= this
        mm_max     = mm.get("max_price", 0.97)    # sniper takes over above this
        mm_gap     = mm.get("gap_cents", 0.02)    # post this far below current price
        mm_conts   = mm.get("contracts_per_side", 10)
        mm_window  = mm.get("window_seconds", 150)
        max_per_market = self.config["capital"].get("max_per_market", 40.0)

        for market in list(self.poly_listener.markets.values()):
            if market.is_expired:
                continue
            if market.seconds_to_expiry > mm_window:
                continue

            side, price = market.best_trade_side
            if price < mm_min or price >= mm_max:
                continue

            # Shared $40 cap with sniper (repeats allowed until cap is hit)
            exposure = self._market_exposure(market.market_id)
            if exposure >= max_per_market:
                continue

            # ── Reversal risk check — only place when direction is strongly confirmed ──
            bd = self.binance.get(market.coin)
            if not bd or not self.binance.is_ready(market.coin):
                continue

            buying_up = (side == "yes")
            mom_60 = bd.momentum(60)

            # 60s momentum must point in the winning direction
            if mom_60 is None:
                continue
            if buying_up and mom_60 <= 0:
                logger.debug(f"MM-BOTH skip {market.coin}: UP but mom60={mom_60:.3f}%")
                continue
            if not buying_up and mom_60 >= 0:
                logger.debug(f"MM-BOTH skip {market.coin}: DOWN but mom60={mom_60:.3f}%")
                continue

            # Volatility check as % of price (not absolute dollars)
            vol = bd.volatility(60)
            max_vol_pct = mm.get("max_volatility_pct", 0.3)  # 0.3% std dev over 60s
            if vol is not None and bd.price > 0:
                vol_pct = (vol / bd.price) * 100
                if vol_pct > max_vol_pct:
                    logger.debug(f"MM-BOTH skip {market.coin}: vol={vol_pct:.3f}% > {max_vol_pct}%")
                    continue

            # ── Prices ─────────────────────────────────────────────────────
            win_price  = round(max(0.01, price - mm_gap), 2)
            lose_price = round(max(0.01, (1.0 - price) - mm_gap), 2)
            win_usdc   = round(win_price  * mm_conts, 2)
            lose_usdc  = round(lose_price * mm_conts, 2)
            total_usdc = win_usdc + lose_usdc

            if exposure + total_usdc > max_per_market:
                continue

            can, _ = self.risk_manager.can_trade(self.order_manager.active_count)
            if not can:
                continue

            win_token  = market.trade_token_id
            lose_token = market.no_token_id if side == "yes" else market.yes_token_id

            # min_fill_price: reject if market has slipped more than 5 cents below target
            min_fill = round(win_price - 0.05, 2)

            logger.info(
                f"MAKER-BOTH [{market.coin} {market.timeframe}] "
                f"WIN {side.upper()}@{win_price:.2f} (min_fill={min_fill:.2f}) x{mm_conts} → "
                f"INSURANCE {'DOWN' if side=='yes' else 'UP'}@{lose_price:.2f} x{mm_conts} "
                f"| tte={market.seconds_to_expiry:.0f}s"
            )

            # Step 1 — FOK on winning side; cancels instantly if not filled at a good price
            win_order, fill_price = await self.execution.place_maker_order(
                token_id=win_token, market_id=market.market_id,
                price=win_price, size=win_usdc,
                min_fill_price=min_fill, mode="maker"
            )
            if not win_order:
                continue  # didn't fill — skip insurance entirely

            # Extra guard: if fill price came back worse than min_fill, abort insurance
            if fill_price is not None and fill_price < min_fill:
                logger.warning(
                    f"MAKER win filled at {fill_price:.4f} < min_fill {min_fill:.2f} — skipping insurance"
                )
                win_pos = ManagedPosition(
                    order=win_order, market=market, mode="maker",
                    entry_usdc=win_usdc, entry_price=fill_price,
                )
                self.order_manager.active_orders[win_order.order_id] = win_pos
                asyncio.create_task(self.order_manager._monitor_order(win_pos))
                self.risk_manager.record_trade_open(win_pos)
                continue

            win_pos = ManagedPosition(
                order=win_order, market=market, mode="maker",
                entry_usdc=win_usdc, entry_price=fill_price or win_price,
            )
            self.order_manager.active_orders[win_order.order_id] = win_pos
            asyncio.create_task(self.order_manager._monitor_order(win_pos))
            self.risk_manager.record_trade_open(win_pos)

            # Step 2 — insurance only placed after win confirmed at acceptable price
            lose_order = await self.execution.place_limit_order(
                token_id=lose_token, market_id=market.market_id,
                price=lose_price, size=lose_usdc, mode="manual"
            )
            if lose_order:
                lose_pos = ManagedPosition(
                    order=lose_order, market=market, mode="maker",
                    entry_usdc=lose_usdc, entry_price=lose_price,
                )
                self.order_manager.active_orders[lose_order.order_id] = lose_pos
                asyncio.create_task(self.order_manager._monitor_order(lose_pos))
                self.risk_manager.record_trade_open(lose_pos)

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

    def _kelly_size(self, price: float, min_price: float, max_size: float) -> float:
        """
        Scale position size by probability confidence.
        At min_price (e.g. 0.97): 50% of max_size.
        At 0.99+: 100% of max_size.
        Linear interpolation between those two points.
        """
        price_range = 0.99 - min_price
        if price_range <= 0:
            return max_size
        fraction = 0.5 + 0.5 * (price - min_price) / price_range
        fraction = max(0.5, min(1.0, fraction))
        return max(1.0, round(max_size * fraction, 2))

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
    async def _keepalive_loop(self):
        """
        Ping every 60s to prevent idle timeouts.
        - Pings localhost to keep the internal event loop warm.
        - Pings the Railway public URL (if set) to generate external inbound
          traffic so Railway never considers the service idle/sleepy.
        """
        import httpx as _httpx
        port = self.config.get("dashboard", {}).get("port", 8080)
        # Railway injects RAILWAY_PUBLIC_DOMAIN automatically — no manual env var needed
        domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "") or os.environ.get("RAILWAY_PUBLIC_URL", "")
        public_url = (f"https://{domain}" if domain and not domain.startswith("http") else domain).rstrip("/")
        await asyncio.sleep(30)  # wait for server to start
        while self._running:
            try:
                async with _httpx.AsyncClient(timeout=5.0) as c:
                    await c.get(f"http://localhost:{port}/ping")
                    if public_url:
                        await c.get(f"{public_url}/ping")
                        logger.debug(f"External keepalive ping → {public_url}/ping")
            except Exception as e:
                logger.debug(f"Keepalive ping failed: {e}")
            await asyncio.sleep(60)

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
                if not p.redeemed
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
                "snipe2": self.config.get("snipe2", {}),
            },
            "prices": self._get_prices(),
        }

    def _get_prices(self) -> dict:
        result = {}
        for coin in ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE"]:
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
