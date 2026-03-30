"""
risk_manager.py
Enforces all risk rules:
- Max concurrent trades
- Max capital deployed
- Kelly-based position sizing
- Consecutive loss tracking
- Volatility spike halt
- Pause / resume from dashboard
- Telegram notifications
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import List, Optional
import httpx

from order_manager import ManagedPosition
from signal_engine import SignalResult, TradeMode

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    market_id: str
    coin: str
    mode: str
    entry_price: float
    entry_usdc: float
    pnl: Optional[float]
    win: Optional[bool]
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "market_id": self.market_id,
            "coin": self.coin,
            "mode": self.mode,
            "entry_price": self.entry_price,
            "entry_usdc": self.entry_usdc,
            "pnl": self.pnl,
            "win": self.win,
            "timestamp": self.timestamp,
        }


@dataclass
class BotMetrics:
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl: float = 0.0
    capital_deployed: float = 0.0
    consecutive_losses: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def win_rate(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return self.wins / self.total_trades * 100

    @property
    def uptime_hours(self) -> float:
        return (time.time() - self.start_time) / 3600

    def to_dict(self) -> dict:
        return {
            "total_trades": self.total_trades,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": round(self.win_rate, 1),
            "total_pnl": round(self.total_pnl, 4),
            "capital_deployed": round(self.capital_deployed, 4),
            "consecutive_losses": self.consecutive_losses,
            "uptime_hours": round(self.uptime_hours, 2),
        }


class RiskManager:
    """
    Single source of truth for all trading permissions and capital allocation.
    """

    def __init__(self, config: dict):
        self.config = config
        self.metrics = BotMetrics()
        self.trade_history: List[TradeRecord] = []
        self._paused = False
        self._halted = False
        self._halt_reason = ""
        self._http: Optional[httpx.AsyncClient] = None
        self._load_config()
        self._init_telegram()

    def _load_config(self):
        cap = self.config.get("capital", {})
        self.total_capital = cap.get("total", 100.0)
        self.max_concurrent = cap.get("max_concurrent_trades", 2)
        self.maker_alloc_pct = cap.get("maker_allocation_pct", 0.30)

        risk = self.config.get("risk", {})
        self.loss_limit = risk.get("consecutive_loss_limit", 3)
        self.vol_spike_threshold = risk.get("volatility_spike_threshold", 2.5)

    def reload_config(self, config: dict):
        self.config = config
        old_capital = self.total_capital
        self._load_config()
        logger.info(
            f"RiskManager config reloaded. Capital: {old_capital} → {self.total_capital}"
        )

    def _init_telegram(self):
        tg = self.config.get("telegram", {})
        if tg.get("enabled") and os.environ.get("TELEGRAM_BOT_TOKEN"):
            self._http = httpx.AsyncClient(timeout=10.0)
            self._tg_token = os.environ["TELEGRAM_BOT_TOKEN"]
            self._tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "")
        else:
            self._http = None

    # ------------------------------------------------------------------
    # PERMISSION GATE
    # ------------------------------------------------------------------
    def can_trade(self, active_count: int) -> tuple[bool, str]:
        if self._halted:
            return False, f"halted: {self._halt_reason}"
        if self._paused:
            return False, "paused by user"
        if self._consecutive_losses() >= self.loss_limit:
            self._halt(f"consecutive_losses >= {self.loss_limit}")
            return False, f"consecutive_losses >= {self.loss_limit}"
        if active_count >= self.max_concurrent:
            return False, f"max_concurrent ({self.max_concurrent}) reached"
        if self.metrics.capital_deployed >= self.total_capital * 0.95:
            return False, "capital fully deployed"
        return True, "ok"

    def position_size(self, signal: SignalResult, active_count: int) -> float:
        """
        Returns USDC amount to allocate for this trade.
        Applies Kelly fraction and enforces per-mode capital limits.
        """
        available = self.total_capital - self.metrics.capital_deployed
        if available <= 0:
            return 0.0

        base_size = self.total_capital * signal.kelly_fraction

        # Maker trades capped at maker_alloc_pct of total capital
        if signal.mode == TradeMode.MAKER:
            max_maker = self.total_capital * self.maker_alloc_pct
            base_size = min(base_size, max_maker)

        # Never exceed available capital
        size = min(base_size, available)

        # Minimum viable trade ($2)
        if size < 2.0:
            return 0.0

        return round(size, 2)

    # ------------------------------------------------------------------
    # TRADE RECORDING
    # ------------------------------------------------------------------
    def record_trade_open(self, pos: ManagedPosition):
        self.metrics.capital_deployed += pos.entry_usdc
        self.metrics.total_trades += 1
        logger.info(
            f"Trade opened: {pos.market.market_id} "
            f"${pos.entry_usdc:.2f} deployed. Total deployed: ${self.metrics.capital_deployed:.2f}"
        )

    def record_trade_closed(self, pos: ManagedPosition, reason: str = "resolved"):
        self.metrics.capital_deployed = max(
            0, self.metrics.capital_deployed - pos.entry_usdc
        )
        pnl = pos.pnl or 0.0
        win = pnl > 0

        self.metrics.total_pnl += pnl
        if win:
            self.metrics.wins += 1
            self.metrics.consecutive_losses = 0
        else:
            self.metrics.losses += 1
            self.metrics.consecutive_losses += 1

        record = TradeRecord(
            market_id=pos.market.market_id,
            coin=pos.market.coin,
            mode=pos.mode,
            entry_price=pos.entry_price,
            entry_usdc=pos.entry_usdc,
            pnl=pnl,
            win=win,
        )
        self.trade_history.append(record)

        asyncio.create_task(self._notify_trade_closed(record))
        logger.info(
            f"Trade closed [{reason}]: {pos.market.market_id} "
            f"PnL={pnl:+.4f} win={win} consecutive_losses={self.metrics.consecutive_losses}"
        )

    def record_cancelled(self, pos: ManagedPosition):
        self.metrics.capital_deployed = max(
            0, self.metrics.capital_deployed - pos.entry_usdc
        )
        asyncio.create_task(self._notify_cancel(pos))

    # ------------------------------------------------------------------
    # PAUSE / RESUME (dashboard control)
    # ------------------------------------------------------------------
    def pause(self):
        self._paused = True
        logger.info("Bot PAUSED — existing positions continue, no new trades")
        asyncio.create_task(self._notify_text("⏸ Bot PAUSED — no new trades"))

    def resume(self):
        self._paused = False
        logger.info("Bot RESUMED")
        asyncio.create_task(self._notify_text("▶️ Bot RESUMED"))

    def emergency_halt(self, reason: str = "manual"):
        self._halt(reason)
        asyncio.create_task(self._notify_text(f"🛑 EMERGENCY HALT: {reason}"))

    def reset_halt(self):
        self._halted = False
        self._halt_reason = ""
        self.metrics.consecutive_losses = 0
        logger.info("Halt reset — bot can trade again")

    @property
    def is_paused(self) -> bool:
        return self._paused

    @property
    def is_halted(self) -> bool:
        return self._halted

    def _halt(self, reason: str):
        self._halted = True
        self._halt_reason = reason
        logger.warning(f"Bot HALTED: {reason}")

    def _consecutive_losses(self) -> int:
        return self.metrics.consecutive_losses

    # ------------------------------------------------------------------
    # TELEGRAM NOTIFICATIONS
    # ------------------------------------------------------------------
    async def _notify_trade_closed(self, record: TradeRecord):
        tg_cfg = self.config.get("telegram", {})
        if not tg_cfg.get("alert_on_trade"):
            return
        emoji = "✅" if record.win else "❌"
        msg = (
            f"{emoji} Trade {'WON' if record.win else 'LOST'}\n"
            f"Market: {record.market_id}\n"
            f"Coin: {record.coin} | Mode: {record.mode}\n"
            f"Entry: {record.entry_price:.4f} | Size: ${record.entry_usdc:.2f}\n"
            f"PnL: {record.pnl:+.4f} USDC\n"
            f"Win rate: {self.metrics.win_rate:.1f}% | "
            f"Total PnL: {self.metrics.total_pnl:+.4f}"
        )
        await self._notify_text(msg)

    async def _notify_cancel(self, pos: ManagedPosition):
        tg_cfg = self.config.get("telegram", {})
        if not tg_cfg.get("alert_on_cancel"):
            return
        msg = (
            f"🚫 Order Cancelled\n"
            f"Market: {pos.market.market_id} | Mode: {pos.mode}\n"
            f"Price: {pos.entry_price:.4f}"
        )
        await self._notify_text(msg)

    async def _notify_text(self, text: str):
        if not self._http or not self._tg_token or not self._tg_chat:
            return
        try:
            await self._http.post(
                f"https://api.telegram.org/bot{self._tg_token}/sendMessage",
                json={"chat_id": self._tg_chat, "text": text, "parse_mode": "HTML"},
            )
        except Exception as e:
            logger.debug(f"Telegram notify failed: {e}")

    async def close(self):
        if self._http:
            await self._http.aclose()
