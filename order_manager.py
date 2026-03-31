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
    opened_at: float = field(default_factory=time.time)
    resolved: bool = False
    redeemed: bool = False
    pnl: Optional[float] = None

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
        asyncio.create_task(self._resolution_watcher())
        logger.info("OrderManager started")

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

        order = await self.execution.place_limit_order(
            token_id=market.trade_token_id,
            market_id=market.market_id,
            price=price,
            size=usdc_size,
            mode=mode,
        )
        if not order:
            return None

        pos = ManagedPosition(
            order=order,
            market=market,
            mode=mode,
            entry_usdc=usdc_size,
            entry_price=price,
        )
        self.active_orders[order.order_id] = pos

        # Spawn the monitor loop for this order
        asyncio.create_task(self._monitor_order(pos))
        return pos

    # ------------------------------------------------------------------
    # MONITOR LOOP — runs per order
    # ------------------------------------------------------------------
    async def _monitor_order(self, pos: ManagedPosition):
        order = pos.order
        market = pos.market
        mode = pos.mode
        timeout = self._timeout_for_mode(mode)

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
    async def _resolution_watcher(self):
        """Checks filled positions every 5s and redeems as soon as market expires."""
        while self._running:
            await asyncio.sleep(5)
            for order_id, pos in list(self.filled_positions.items()):
                if pos.redeemed:
                    continue
                if pos.market.is_expired:
                    logger.info(f"Market expired — redeeming position {order_id[:16]}...")
                    await self._attempt_redeem(pos)

    async def _attempt_redeem(self, pos: ManagedPosition):
        shares = pos.order.filled_size
        if shares <= 0:
            logger.warning(f"_attempt_redeem: position {pos.order.order_id[:16]} has 0 shares")
            return

        if not pos.market.condition_id:
            logger.error(
                f"_attempt_redeem: no condition_id for market {pos.market.market_id[:16]} "
                f"— cannot redeem. Redeem manually on polymarket.com"
            )
            return

        # Determine yes/no amounts based on which token was bought
        is_yes = pos.order.token_id == pos.market.yes_token_id
        yes_amount = int(shares * 1e6) if is_yes else 0
        no_amount  = int(shares * 1e6) if not is_yes else 0

        logger.info(
            f"Attempting redeem for {pos.order.order_id[:16]} "
            f"shares={shares:.4f} is_yes={is_yes} "
            f"condition={pos.market.condition_id[:16]}..."
        )

        success = await self.execution.redeem_position(
            condition_id=pos.market.condition_id,
            amounts=[yes_amount, no_amount],
        )

        if success:
            proceeds = shares * 1.0
            pos.mark_redeemed(proceeds)
            if self.on_redeem:
                self.on_redeem(pos)
        else:
            logger.warning(
                f"Redeem failed for {pos.order.order_id[:16]} "
                f"— will retry next cycle. Redeem manually on polymarket.com if needed."
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
            "maker": self.MAKER_TIMEOUT,
        }.get(mode, self.STANDARD_TIMEOUT)

    @property
    def active_count(self) -> int:
        return len(self.active_orders)

    @property
    def open_positions(self) -> List[ManagedPosition]:
        return list(self.filled_positions.values())

    def get_all_positions(self) -> List[ManagedPosition]:
        return list(self.active_orders.values()) + list(self.filled_positions.values())
