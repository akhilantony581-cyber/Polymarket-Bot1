"""
order_manager.py
Manages the full lifecycle of every order:
  PENDING → OPEN → FILLED → REDEEMED
  PENDING → OPEN → CANCELLED (signal change / timeout)

Handles:
- Timeout enforcement per mode
- Sniper repricing loop
- Auto-redeem after resolution
- Signal-change cancellation
- Partial fill tracking
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from execution_engine import ExecutionEngine, OrderStatus, PlacedOrder
from signal_engine import SignalEngine, TradeMode
from polymarket_listener import PolymarketMarket, PolymarketListener

logger = logging.getLogger(__name__)


@dataclass
class ManagedPosition:
    order: PlacedOrder
    market: PolymarketMarket
    mode: str
    entry_usdc: float           # USDC spent
    entry_price: float
    trade_side: str = "yes"     # 'yes' or 'no' — which token was bought
    opened_at: float = field(default_factory=time.time)
    resolved: bool = False
    redeemed: bool = False
    pnl: Optional[float] = None
    last_redeem_attempt: float = 0.0   # timestamp of last redeem attempt

    @property
    def is_filled(self) -> bool:
        return self.order.status == OrderStatus.FILLED

    @property
    def shares_held(self) -> float:
        return self.order.filled_size

    def mark_redeemed(self, proceeds: float):
        self.redeemed = True
        self.resolved = True
        self.pnl = proceeds - self.entry_usdc
        logger.info(
            f"Position redeemed: {self.order.market_id} "
            f"PnL={self.pnl:+.4f} USDC entry={self.entry_usdc:.4f}"
        )


class OrderManager:
    """
    Supervises all active orders and positions.
    Runs per-order monitor loops that enforce timeouts and repricing.
    """

    STANDARD_TIMEOUT = 20    # seconds
    SNIPER_TIMEOUT = 5       # seconds
    MAKER_TIMEOUT = 7200     # seconds
    SNIPER_REPRICE_INTERVAL = 3  # seconds

    def __init__(
        self,
        config: dict,
        execution: ExecutionEngine,
        signal_engine: SignalEngine,
        poly_listener: PolymarketListener,
        on_fill: Optional[Callable] = None,
        on_cancel: Optional[Callable] = None,
        on_redeem: Optional[Callable] = None,
    ):
        self.config = config
        self.execution = execution
        self.signal_engine = signal_engine
        self.poly_listener = poly_listener

        # Callbacks for risk manager and logging
        self.on_fill = on_fill
        self.on_cancel = on_cancel
        self.on_redeem = on_redeem

        self.active_orders: Dict[str, ManagedPosition] = {}  # order_id → position
        self.filled_positions: Dict[str, ManagedPosition] = {}  # order_id → position
        self._running = False

    def _load_timeouts(self):
        std_cfg = self.config.get("standard", {})
        snp_cfg = self.config.get("sniper", {})
        mkr_cfg = self.config.get("maker", {})
        self.STANDARD_TIMEOUT = std_cfg.get("order_timeout_seconds", 20)
        self.SNIPER_TIMEOUT = snp_cfg.get("order_timeout_seconds", 5)
        self.MAKER_TIMEOUT = mkr_cfg.get("order_timeout_seconds", 7200)
        self.SNIPER_REPRICE_INTERVAL = snp_cfg.get("reprice_interval_seconds", 3)

    async def start(self):
        self._running = True
        self._load_timeouts()
        asyncio.create_task(self._supervised_loop(self._resolution_watcher, "resolution_watcher"))
        logger.info("OrderManager started")

    async def _supervised_loop(self, coro_func, name: str):
        """Restart coro_func if it ever crashes, so it never silently dies."""
        while self._running:
            try:
                await coro_func()
            except Exception as e:
                logger.error(f"[WATCHDOG] {name} crashed: {e} — restarting in 3s", exc_info=True)
                await asyncio.sleep(3)

    async def stop(self):
        self._running = False

    # ------------------------------------------------------------------
    # SUBMIT NEW ORDER
    # ------------------------------------------------------------------
    async def submit(
        self,
        market: PolymarketMarket,
        price: float,
        usdc_size: float,
        mode: str,
    ) -> Optional[ManagedPosition]:

        if mode == "snipe2":
            # FOK (Fill or Kill) = market order: fills at best ask or cancels instantly.
            # Pass price as min_price so engine verifies the real token price hasn't
            # dropped below the threshold between the scan check and submission.
            order = await self.execution.place_market_order(
                token_id=market.trade_token_id,
                market_id=market.market_id,
                size=usdc_size,
                min_price=price,
            )
        else:
            order = await self.execution.place_limit_order(
                token_id=market.trade_token_id,
                market_id=market.market_id,
                price=price,
                size=usdc_size,
                mode=mode,
            )
        if not order:
            return None

        trade_side, _ = market.best_trade_side
        pos = ManagedPosition(
            order=order,
            market=market,
            mode=mode,
            entry_usdc=usdc_size,
            entry_price=price,
            trade_side=trade_side,
        )
        self.active_orders[order.order_id] = pos

        # Spawn the monitor loop for this order
        asyncio.create_task(self._monitor_order(pos))
        return pos

    # ------------------------------------------------------------------
    # MONITOR LOOP — runs per order
    # ------------------------------------------------------------------
    async def _monitor_order(self, pos: ManagedPosition):
        try:
            await self._monitor_order_inner(pos)
        except Exception as e:
            logger.error(f"Monitor crashed for {pos.order.order_id}: {e} — force-cancelling")
            self._on_order_cancelled(pos, "monitor_crash")

    async def _monitor_order_inner(self, pos: ManagedPosition):
        order = pos.order
        market = pos.market
        mode = pos.mode
        # snipe2 uses FOK: fills or cancels in milliseconds — just poll once quickly
        if mode == "snipe2":
            await asyncio.sleep(1.0)
            status = await self.execution.get_order_status(order)
            if status == OrderStatus.FILLED:
                self._on_order_filled(pos)
            else:
                order.status = OrderStatus.CANCELLED
                self.active_orders.pop(order.order_id, None)
                logger.info(f"SNIPE2 FOK not filled (cancelled): {order.order_id}")
            return

        # For sniper: timeout = min(config, seconds_to_expiry - 2) so order
        # stays alive right up to market resolution without outlasting it.
        base_timeout = self._timeout_for_mode(mode)
        if mode == "sniper":
            tte = market.seconds_to_expiry
            timeout = min(base_timeout, max(tte - 2, 5))
        else:
            timeout = base_timeout

        logger.info(f"Monitoring order {order.order_id} mode={mode} timeout={timeout}s")

        while self._running and order.is_active:
            await asyncio.sleep(
                self.config.get("execution", {}).get("order_check_interval", 1.0)
            )

            # Refresh order status
            status = await self.execution.get_order_status(order)

            if status == OrderStatus.FILLED:
                self._on_order_filled(pos)
                return

            if status == OrderStatus.CANCELLED:
                self._on_order_cancelled(pos, "external_cancel")
                return

            # Timeout check
            if order.age_seconds > timeout:
                logger.info(f"Order {order.order_id} timed out after {timeout}s")
                await self.execution.cancel_order(order)
                self._on_order_cancelled(pos, "timeout")
                return

            # Signal reversal check (standard and maker only)
            if mode in ("standard", "maker"):
                refreshed = self.poly_listener.get_market(market.market_id)
                if refreshed:
                    pos.market = refreshed
                    if refreshed.yes_price < self.config.get("price", {}).get("min_entry", 0.98):
                        logger.info(f"YES price dropped below floor — cancelling {order.order_id}")
                        await self.execution.cancel_order(order)
                        self._on_order_cancelled(pos, "price_below_floor")
                        return

                    sig = self.signal_engine.evaluate(refreshed)
                    if not sig.is_tradeable():
                        logger.info(
                            f"Signal reversed — cancelling {order.order_id}: {sig.reason}"
                        )
                        await self.execution.cancel_order(order)
                        self._on_order_cancelled(pos, f"signal_reversed:{sig.reason}")
                        return

            # Sniper repricing
            if mode == "sniper" and order.age_seconds % self.SNIPER_REPRICE_INTERVAL < 1.0:
                await self._reprice_sniper(pos)

    async def _reprice_sniper(self, pos: ManagedPosition):
        best_ask = await self.execution.get_best_ask(pos.market.yes_token_id)
        if not best_ask:
            return
        min_entry = self.config.get("price", {}).get("min_entry", 0.98)
        if best_ask < min_entry:
            logger.info(f"Sniper best ask {best_ask} below floor — aborting reprice")
            return
        if abs(best_ask - pos.order.price) > 0.001:
            new_order = await self.execution.reprice_order(
                pos.order,
                new_price=best_ask,
                token_id=pos.market.yes_token_id,
                market_id=pos.market.market_id,
            )
            if new_order:
                del self.active_orders[pos.order.order_id]
                pos.order = new_order
                pos.entry_price = new_order.price
                self.active_orders[new_order.order_id] = pos
                logger.info(f"Sniper repriced to {best_ask}")

    # ------------------------------------------------------------------
    # FILL / CANCEL HANDLERS
    # ------------------------------------------------------------------
    def _on_order_filled(self, pos: ManagedPosition):
        del self.active_orders[pos.order.order_id]
        self.filled_positions[pos.order.order_id] = pos
        logger.info(
            f"Order filled: {pos.order.order_id} @ {pos.order.price} "
            f"shares={pos.order.filled_size}"
        )
        if self.on_fill:
            self.on_fill(pos)

    def _on_order_cancelled(self, pos: ManagedPosition, reason: str):
        self.active_orders.pop(pos.order.order_id, None)
        logger.info(f"Order cancelled [{reason}]: {pos.order.order_id}")
        if self.on_cancel:
            self.on_cancel(pos, reason)

    # ------------------------------------------------------------------
    # RESOLUTION WATCHER — auto-redeem
    # ------------------------------------------------------------------
    REDEEM_RETRY_INTERVAL = 60  # seconds between redeem retries (avoid 429)

    async def _resolution_watcher(self):
        """
        Every 60s: fetch ALL redeemable positions from Polymarket data API
        and redeem them. Covers positions from current and previous sessions.
        """
        import httpx as _httpx
        proxy_wallet = os.environ.get("POLYMARKET_PROXY_WALLET", "")
        _last_api_sweep = 0.0

        while self._running:
            try:
                await asyncio.sleep(10)

                # Redeem current-session filled positions as they expire
                for order_id, pos in list(self.filled_positions.items()):
                    if pos.redeemed or not pos.market.is_expired:
                        continue
                    if time.time() - pos.last_redeem_attempt < self.REDEEM_RETRY_INTERVAL:
                        continue
                    logger.info(f"Market expired — redeeming position {order_id[:16]}...")
                    pos.last_redeem_attempt = time.time()
                    await self._attempt_redeem(pos)
                    await asyncio.sleep(2)

                # Every 60s: sweep Polymarket API for any redeemable positions
                if not proxy_wallet or time.time() - _last_api_sweep < 60:
                    continue
                _last_api_sweep = time.time()
                try:
                    async with _httpx.AsyncClient(timeout=10.0) as client:
                        resp = await client.get(
                            "https://data-api.polymarket.com/positions",
                            params={"user": proxy_wallet, "redeemable": "true", "limit": 500},
                        )
                    positions = resp.json() if resp.status_code == 200 else []
                    if positions:
                        logger.info(f"Auto-redeem: {len(positions)} redeemable position(s) found")
                    for p in positions:
                        cid = p.get("conditionId") or p.get("condition_id", "")
                        if not cid:
                            continue
                        # Match against tracked positions so logging works correctly
                        match = next(
                            (fp for fp in self.filled_positions.values()
                             if getattr(fp.market, "condition_id", "") == cid and not fp.redeemed),
                            None
                        )
                        if match:
                            await self._attempt_redeem(match)
                        else:
                            # Untracked position (previous session) — redeem and update DB
                            proceeds = await self.execution.redeem_position(cid, [])
                            if proceeds is not None:
                                logger.info(
                                    f"Redeemed untracked position cid={cid[:16]} "
                                    f"proceeds={proceeds:.4f}"
                                )
                                try:
                                    import trade_db as _tdb
                                    win = 1 if proceeds > 0 else 0
                                    # Estimate pnl from API data if available
                                    init_val = float(
                                        p.get("initialValue") or
                                        p.get("investedAmount") or
                                        p.get("initValue") or 0
                                    )
                                    pnl = round(proceeds - init_val, 6) if init_val > 0 else None
                                    _tdb.mark_resolved_by_market(cid, win, pnl)
                                except Exception as _e:
                                    logger.debug(f"DB update for untracked position failed: {_e}")
                        await asyncio.sleep(3)
                except Exception as e:
                    logger.debug(f"Auto-redeem sweep error: {e}")

            except Exception as e:
                logger.error(f"Resolution watcher iteration error: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def _attempt_redeem(self, pos: ManagedPosition):
        if not pos.market.condition_id:
            logger.error(
                f"_attempt_redeem: no condition_id for {pos.order.order_id[:16]} "
                f"— redeem manually on polymarket.com"
            )
            return

        logger.info(
            f"Attempting redeem for {pos.order.order_id[:16]} "
            f"condition={pos.market.condition_id[:16]}..."
        )

        # execution.redeem_position() now returns the actual USDC received from
        # the on-chain Transfer log — 0.0 for a loss, >0 for a win. No guessing.
        proceeds = await self.execution.redeem_position(
            condition_id=pos.market.condition_id,
            amounts=[],  # unused — CTF redeems all held tokens
        )

        if proceeds is not None:
            pos.mark_redeemed(proceeds)
            logger.info(
                f"Redeem complete: {pos.order.order_id[:16]} "
                f"proceeds={proceeds:.4f} pnl={pos.pnl:+.4f}"
            )
            if self.on_redeem:
                self.on_redeem(pos)
        else:
            logger.warning(
                f"Redeem failed for {pos.order.order_id[:16]} "
                f"— will retry in 60s. Redeem manually on polymarket.com if needed."
            )

    # ------------------------------------------------------------------
    # MANUAL EXIT (dashboard/Telegram command)
    # ------------------------------------------------------------------
    async def manual_exit_position(self, order_id: str, exit_price: float) -> bool:
        pos = self.filled_positions.get(order_id)
        if not pos:
            logger.warning(f"manual_exit: position {order_id} not found")
            return False

        shares = pos.order.filled_size
        exit_order = await self.execution.manual_exit(
            token_id=pos.market.yes_token_id,
            market_id=pos.market.market_id,
            size=shares,
            exit_price=exit_price,
        )
        if exit_order:
            asyncio.create_task(self._monitor_exit(pos, exit_order))
            return True
        return False

    async def _monitor_exit(self, pos: ManagedPosition, exit_order: PlacedOrder):
        for _ in range(30):
            await asyncio.sleep(2)
            status = await self.execution.get_order_status(exit_order)
            if status == OrderStatus.FILLED:
                proceeds = exit_order.filled_size * exit_order.price
                pos.mark_redeemed(proceeds)
                if self.on_redeem:
                    self.on_redeem(pos)
                return
        logger.warning(f"Manual exit order not filled after 60s: {exit_order.order_id}")

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------
    def _timeout_for_mode(self, mode: str) -> int:
        return {
            "standard": self.STANDARD_TIMEOUT,
            "sniper": self.SNIPER_TIMEOUT,
            "snipe2": self.SNIPER_TIMEOUT,
            "maker": self.MAKER_TIMEOUT,
            "copy": 120,   # 2-minute window for copy-trade limit orders
        }.get(mode, self.STANDARD_TIMEOUT)

    @property
    def active_count(self) -> int:
        return len(self.active_orders)

    @property
    def open_positions(self) -> List[ManagedPosition]:
        return list(self.filled_positions.values())

    def get_all_positions(self) -> List[ManagedPosition]:
        return list(self.active_orders.values()) + list(self.filled_positions.values())
