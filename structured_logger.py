"""
structured_logger.py
Writes structured JSONL logs for trades, signals, and metrics.
Used for post-hoc analysis and dashboard data.
"""

import json
import logging
import time
from pathlib import Path
from typing import Any

from signal_engine import SignalResult

logger = logging.getLogger(__name__)


class StructuredLogger:
    def __init__(self, config: dict):
        log_cfg = config.get("logging", {})
        self._trade_path = Path(log_cfg.get("trade_log_file", "logs/trades.jsonl"))
        self._signal_path = Path(log_cfg.get("signal_log_file", "logs/signals.jsonl"))
        self._metrics_path = Path(log_cfg.get("metrics_log_file", "logs/metrics.jsonl"))

        for p in (self._trade_path, self._signal_path, self._metrics_path):
            p.parent.mkdir(exist_ok=True)

    def log_signal(self, signal: SignalResult):
        # Only log signals that were considered (not silent rejections)
        if signal.yes_price >= 0.97:
            self._write(self._signal_path, signal.to_dict())

    def log_trade_open(self, pos: Any):
        self._write(self._trade_path, {
            "event": "open",
            "order_id": pos.order.order_id,
            "market_id": pos.market.market_id,
            "coin": pos.market.coin,
            "timeframe": pos.market.timeframe,
            "mode": pos.mode,
            "entry_price": pos.entry_price,
            "entry_usdc": pos.entry_usdc,
            "timestamp": time.time(),
        })

    def log_trade_closed(self, pos: Any):
        self._write(self._trade_path, {
            "event": "closed",
            "order_id": pos.order.order_id,
            "market_id": pos.market.market_id,
            "coin": pos.market.coin,
            "mode": pos.mode,
            "entry_price": pos.entry_price,
            "entry_usdc": pos.entry_usdc,
            "pnl": pos.pnl,
            "win": (pos.pnl or 0) > 0,
            "redeemed": pos.redeemed,
            "timestamp": time.time(),
        })

    def log_cancel(self, pos: Any, reason: str):
        self._write(self._trade_path, {
            "event": "cancelled",
            "order_id": pos.order.order_id,
            "market_id": pos.market.market_id,
            "coin": pos.market.coin,
            "mode": pos.mode,
            "entry_price": pos.entry_price,
            "reason": reason,
            "timestamp": time.time(),
        })

    def log_metrics(self, metrics: dict):
        self._write(self._metrics_path, {**metrics, "timestamp": time.time()})

    def _write(self, path: Path, data: dict):
        try:
            with open(path, "a") as f:
                f.write(json.dumps(data) + "\n")
        except Exception as e:
            logger.warning(f"Structured log write failed: {e}")
