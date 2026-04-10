"""
arb_bot.py
Bot 3 — Locked Arbitrage Market Maker for 1h crypto markets.

Strategy: buy EQUAL CONTRACTS of YES + NO simultaneously.
  Cost per arb  = N × (YES_ask + NO_ask)     [USDC]
  Payout        = N × $1.00 × 0.98           [after 2% redemption fee]
  Profit        = N × (0.98 – YES_ask – NO_ask)  [guaranteed, zero directional risk]

Fires when YES_ask + NO_ask ≤ arb_threshold (default 0.97 → ≥1% guaranteed profit).
Uses GTC limit orders at best ask; 60s fill timeout with cancel-partial logic.

Passive mode: posts GTC bids at midpoint − offset on both sides for slower fills.
"""

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ArbLeg:
    token_id: str
    side: str                   # "yes" or "no"
    ask_price: float
    contracts: float            # number of contracts (shares)
    usdc_cost: float            # contracts × ask_price
    order_id: Optional[str] = None
    filled: bool = False
    cancelled: bool = False
    placed_at: float = field(default_factory=time.time)


@dataclass
class ArbPosition:
    arb_id: str
    market_id: str
    coin: str
    timeframe: str
    yes_leg: ArbLeg
    no_leg: ArbLeg
    mode: str = "arb"           # "arb" (aggressive) | "passive" (passive posting)
    status: str = "pending"     # pending | monitoring | partial | complete | cancelled
    guaranteed_profit: float = 0.0
    opened_at: float = field(default_factory=time.time)

    @property
    def total_cost(self) -> float:
        return self.yes_leg.usdc_cost + self.no_leg.usdc_cost

    @property
    def age_seconds(self) -> float:
        return time.time() - self.opened_at

    @property
    def combined_ask(self) -> float:
        return self.yes_leg.ask_price + self.no_leg.ask_price


class ArbBot:
    """
    Scans all active 1h crypto markets every second.
    When YES_ask + NO_ask ≤ threshold, buys both sides simultaneously (GTC limit).
    Monitors fills for up to fill_timeout_seconds; cancels unfilled legs.
    """

    def __init__(self, config: dict, execution, poly_listener):
        self.config = config
        self.execution = execution
        self.poly_listener = poly_listener

        self._running = False

        # Live state
        self.active_arbs: Dict[str, ArbPosition] = {}   # arb_id → position
        self.complete_arbs: List[ArbPosition] = []       # filled (locked profit)
        self.cancelled_arbs: List[ArbPosition] = []      # partial fills that were cancelled

        # Scan stats
        self._scan_count = 0
        self._last_scan_at = 0.0
        self._opportunities_seen = 0

        # Per-market cooldown: skip re-entering the same market within cooldown_seconds
        self._market_last_arb: Dict[str, float] = {}

    # ------------------------------------------------------------------ #
    #  LIFECYCLE                                                           #
    # ------------------------------------------------------------------ #
    async def start(self):
        self._running = True
        logger.info("[ArbBot] Starting — locked arbitrage scanner active")
        asyncio.create_task(self._arb_scan_loop())

    async def stop(self):
        self._running = False
        logger.info("[ArbBot] Stopped")

    # ------------------------------------------------------------------ #
    #  MAIN SCAN LOOP                                                      #
    # ------------------------------------------------------------------ #
    async def _arb_scan_loop(self):
        while self._running:
            try:
                await self._scan_once()
            except Exception as e:
                logger.error(f"[ArbBot] Scan error: {e}", exc_info=True)
            await asyncio.sleep(1.0)

    async def _scan_once(self):
        cfg = self.config.get("arb_bot", {})
        if not cfg.get("enabled", True):
            return

        threshold      = cfg.get("arb_threshold",      0.97)
        size_usdc      = cfg.get("size_usdc",           50.0)
        max_concurrent = cfg.get("max_concurrent_arbs", 4)
        min_tte        = cfg.get("min_tte_seconds",     300)
        cooldown       = cfg.get("market_cooldown_seconds", 120)

        # Cap active arbs
        active_count = sum(1 for a in self.active_arbs.values() if a.status in ("pending", "monitoring"))
        if active_count >= max_concurrent:
            return

        self._scan_count += 1
        self._last_scan_at = time.time()

        for market in list(self.poly_listener.markets.values()):
            # Only 1h markets with enough time remaining
            if market.timeframe != "1h":
                continue
            if market.is_expired:
                continue
            tte = market.seconds_to_expiry
            if tte < min_tte:
                continue

            # Per-market cooldown (avoid re-entering too soon)
            last_arb = self._market_last_arb.get(market.market_id, 0.0)
            if time.time() - last_arb < cooldown:
                continue

            # Already have an active arb on this market?
            if any(a.market_id == market.market_id and a.status in ("pending", "monitoring")
                   for a in self.active_arbs.values()):
                continue

            # Fetch fresh YES and NO best asks from CLOB
            yes_ask, no_ask = await self._fetch_both_asks(market)
            if yes_ask is None or no_ask is None:
                continue
            if yes_ask <= 0 or no_ask <= 0:
                continue

            combined = yes_ask + no_ask
            if combined > threshold:
                continue

            # Arb opportunity found!
            self._opportunities_seen += 1
            guaranteed_profit = size_usdc * (0.98 - combined) / combined
            logger.info(
                f"[ArbBot] ARB OPPORTUNITY {market.coin} {market.timeframe} "
                f"YES={yes_ask:.4f} NO={no_ask:.4f} combined={combined:.4f} "
                f"threshold={threshold:.3f} "
                f"guaranteed_profit=${guaranteed_profit:.3f} tte={tte:.0f}s"
            )

            await self._execute_arb(market, yes_ask, no_ask, size_usdc)

            # Only execute one arb per scan cycle
            active_count += 1
            if active_count >= max_concurrent:
                break

    # ------------------------------------------------------------------ #
    #  EXECUTE ARB — both sides simultaneously                             #
    # ------------------------------------------------------------------ #
    async def _execute_arb(self, market, yes_ask: float, no_ask: float, total_usdc: float):
        combined = yes_ask + no_ask
        # N contracts: buying equal contracts on both sides
        n_contracts = round(total_usdc / combined, 6)

        yes_cost = round(n_contracts * yes_ask, 4)
        no_cost  = round(n_contracts * no_ask,  4)
        guaranteed_profit = round(n_contracts * (0.98 - combined), 4)

        arb_id = str(uuid.uuid4())[:12]
        yes_leg = ArbLeg(
            token_id=market.yes_token_id,
            side="yes",
            ask_price=yes_ask,
            contracts=n_contracts,
            usdc_cost=yes_cost,
        )
        no_leg = ArbLeg(
            token_id=market.no_token_id,
            side="no",
            ask_price=no_ask,
            contracts=n_contracts,
            usdc_cost=no_cost,
        )
        arb_pos = ArbPosition(
            arb_id=arb_id,
            market_id=market.market_id,
            coin=market.coin,
            timeframe=market.timeframe,
            yes_leg=yes_leg,
            no_leg=no_leg,
            mode="arb",
            status="pending",
            guaranteed_profit=guaranteed_profit,
        )
        self.active_arbs[arb_id] = arb_pos
        self._market_last_arb[market.market_id] = time.time()

        logger.info(
            f"[ArbBot] Executing arb {arb_id} — "
            f"{market.coin} YES@{yes_ask:.4f} x{n_contracts:.2f} (${yes_cost:.2f}) + "
            f"NO@{no_ask:.4f} x{n_contracts:.2f} (${no_cost:.2f}) "
            f"= ${yes_cost+no_cost:.2f} total | locked_profit=${guaranteed_profit:.3f}"
        )

        # Place both orders simultaneously
        yes_order_task = asyncio.create_task(
            self.execution.place_limit_order(
                token_id=market.yes_token_id,
                market_id=market.market_id,
                price=yes_ask,
                size=yes_cost,
                mode="arb",
            )
        )
        no_order_task = asyncio.create_task(
            self.execution.place_limit_order(
                token_id=market.no_token_id,
                market_id=market.market_id,
                price=no_ask,
                size=no_cost,
                mode="arb",
            )
        )
        yes_order, no_order = await asyncio.gather(yes_order_task, no_order_task)

        if yes_order:
            yes_leg.order_id = yes_order.order_id
        if no_order:
            no_leg.order_id = no_order.order_id

        if not yes_order and not no_order:
            logger.warning(f"[ArbBot] {arb_id} — BOTH legs failed to place")
            arb_pos.status = "cancelled"
            self.cancelled_arbs.append(arb_pos)
            del self.active_arbs[arb_id]
            return

        if not yes_order or not no_order:
            # One leg placed, one failed — cancel the placed one immediately
            placed = yes_order or no_order
            failed_side = "YES" if not yes_order else "NO"
            logger.warning(
                f"[ArbBot] {arb_id} — {failed_side} leg failed to place; "
                f"cancelling {placed.order_id}"
            )
            await self.execution.cancel_order(placed)
            arb_pos.status = "cancelled"
            self.cancelled_arbs.append(arb_pos)
            del self.active_arbs[arb_id]
            return

        arb_pos.status = "monitoring"
        # Monitor fills in background
        asyncio.create_task(self._monitor_arb(arb_pos, yes_order, no_order))

    # ------------------------------------------------------------------ #
    #  MONITOR ARB — wait for both legs to fill                            #
    # ------------------------------------------------------------------ #
    async def _monitor_arb(self, arb_pos: ArbPosition, yes_order, no_order):
        cfg = self.config.get("arb_bot", {})
        timeout = cfg.get("fill_timeout_seconds", 60)
        poll_interval = 3.0

        deadline = time.time() + timeout
        from execution_engine import OrderStatus

        while time.time() < deadline and self._running:
            await asyncio.sleep(poll_interval)

            # Check both order statuses
            yes_status = await self.execution.get_order_status(yes_order)
            no_status  = await self.execution.get_order_status(no_order)

            yes_filled = (yes_status == OrderStatus.FILLED)
            no_filled  = (no_status  == OrderStatus.FILLED)

            arb_pos.yes_leg.filled = yes_filled
            arb_pos.no_leg.filled  = no_filled

            if yes_filled and no_filled:
                arb_pos.status = "complete"
                self.complete_arbs.append(arb_pos)
                del self.active_arbs[arb_pos.arb_id]
                logger.info(
                    f"[ArbBot] ARB COMPLETE {arb_pos.arb_id} "
                    f"{arb_pos.coin} — locked profit ${arb_pos.guaranteed_profit:.3f} "
                    f"cost=${arb_pos.total_cost:.2f}"
                )
                return

        # Timeout — cancel any unfilled legs
        logger.warning(
            f"[ArbBot] {arb_pos.arb_id} timed out after {timeout}s — "
            f"YES_filled={arb_pos.yes_leg.filled} NO_filled={arb_pos.no_leg.filled}"
        )

        cancel_tasks = []
        if not arb_pos.yes_leg.filled and yes_order.is_active:
            cancel_tasks.append(self.execution.cancel_order(yes_order))
        if not arb_pos.no_leg.filled and no_order.is_active:
            cancel_tasks.append(self.execution.cancel_order(no_order))

        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)

        # Mark status
        if arb_pos.yes_leg.filled or arb_pos.no_leg.filled:
            arb_pos.status = "partial"
            logger.warning(
                f"[ArbBot] PARTIAL ARB {arb_pos.arb_id} — "
                f"one leg filled, other cancelled. Directional exposure taken!"
            )
        else:
            arb_pos.status = "cancelled"
            logger.info(f"[ArbBot] {arb_pos.arb_id} — no fills, both cancelled cleanly")

        self.cancelled_arbs.append(arb_pos)
        self.active_arbs.pop(arb_pos.arb_id, None)

    # ------------------------------------------------------------------ #
    #  FETCH BEST ASKS FOR BOTH SIDES                                      #
    # ------------------------------------------------------------------ #
    async def _fetch_both_asks(self, market) -> tuple:
        """
        Fetch real-time YES and NO best asks from CLOB book endpoint.
        Falls back to Gamma-derived prices if CLOB fetch fails.
        Returns (yes_ask, no_ask) or (None, None) on error.
        """
        try:
            yes_task = asyncio.create_task(
                self.execution.get_best_ask(market.yes_token_id)
            )
            no_task = asyncio.create_task(
                self.execution.get_best_ask(market.no_token_id)
            )
            yes_ask, no_ask = await asyncio.gather(yes_task, no_task)
            return yes_ask, no_ask
        except Exception as e:
            logger.debug(f"[ArbBot] CLOB fetch failed for {market.coin}: {e}")
            # Fallback to Gamma-derived prices
            return market.yes_price or None, market.no_price or None

    # ------------------------------------------------------------------ #
    #  STATE FOR DASHBOARD                                                 #
    # ------------------------------------------------------------------ #
    def get_state(self) -> dict:
        cfg = self.config.get("arb_bot", {})
        total_locked = sum(a.guaranteed_profit for a in self.complete_arbs)
        total_cost   = sum(a.total_cost for a in self.complete_arbs)

        active_list = [
            {
                "arb_id":            a.arb_id,
                "coin":              a.coin,
                "market_id":         a.market_id,
                "yes_ask":           a.yes_leg.ask_price,
                "no_ask":            a.no_leg.ask_price,
                "combined":          round(a.combined_ask, 4),
                "contracts":         round(a.yes_leg.contracts, 2),
                "total_cost":        round(a.total_cost, 2),
                "locked_profit":     round(a.guaranteed_profit, 4),
                "status":            a.status,
                "yes_filled":        a.yes_leg.filled,
                "no_filled":         a.no_leg.filled,
                "age_seconds":       round(a.age_seconds, 0),
            }
            for a in self.active_arbs.values()
        ]

        recent_complete = [
            {
                "arb_id":        a.arb_id,
                "coin":          a.coin,
                "combined":      round(a.combined_ask, 4),
                "total_cost":    round(a.total_cost, 2),
                "locked_profit": round(a.guaranteed_profit, 4),
                "status":        a.status,
            }
            for a in self.complete_arbs[-20:]
        ]

        return {
            "enabled":              cfg.get("enabled", True),
            "arb_threshold":        cfg.get("arb_threshold", 0.97),
            "size_usdc":            cfg.get("size_usdc", 50.0),
            "max_concurrent_arbs":  cfg.get("max_concurrent_arbs", 4),
            "active_arbs":          active_list,
            "total_complete":       len(self.complete_arbs),
            "total_cancelled":      len(self.cancelled_arbs),
            "total_locked_profit":  round(total_locked, 4),
            "total_cost_deployed":  round(total_cost, 2),
            "scan_count":           self._scan_count,
            "opportunities_seen":   self._opportunities_seen,
            "last_scan_at":         self._last_scan_at,
            "recent_complete":      recent_complete,
        }
