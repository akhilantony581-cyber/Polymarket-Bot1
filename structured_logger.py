"""
structured_logger.py
Writes structured JSONL logs for trades, signals, and metrics.
Used for post-hoc analysis and dashboard data.
"""

import json
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict

from signal_engine import SignalResult

try:
    import trade_db as _tdb
except Exception:
    _tdb = None

logger = logging.getLogger(__name__)


class WinRateTracker:
    """
    Tracks win/loss stats per coin, timeframe, and mode in memory.
    Hydrates from trades.jsonl on startup so stats survive restarts.
    """

    def __init__(self, trade_log_path: Path):
        self._path = trade_log_path
        # key: (coin, timeframe, mode) → {"wins": int, "losses": int, "pnl": float}
        self._stats: Dict[tuple, dict] = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
        self._hydrate()

    def _hydrate(self):
        if not self._path.exists():
            return
        try:
            with open(self._path) as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        if rec.get("event") == "closed" and rec.get("pnl") is not None:
                            self._record(rec["coin"], rec.get("timeframe", "?"), rec.get("mode", "?"), rec["pnl"])
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"WinRateTracker hydrate error: {e}")

    def _record(self, coin: str, timeframe: str, mode: str, pnl: float):
        key = (coin, timeframe, mode)
        self._stats[key]["pnl"] = round(self._stats[key]["pnl"] + pnl, 4)
        if pnl > 0:
            self._stats[key]["wins"] += 1
        else:
            self._stats[key]["losses"] += 1

    def record_closed(self, pos: Any):
        pnl = pos.pnl or 0.0
        self._record(pos.market.coin, pos.market.timeframe, pos.mode, pnl)

    def get_stats(self) -> list:
        rows = []
        for (coin, tf, mode), s in sorted(self._stats.items()):
            total = s["wins"] + s["losses"]
            win_rate = round(s["wins"] / total * 100, 1) if total else 0.0
            rows.append({
                "coin": coin,
                "timeframe": tf,
                "mode": mode,
                "wins": s["wins"],
                "losses": s["losses"],
                "total": total,
                "win_rate_pct": win_rate,
                "pnl": round(s["pnl"], 4),
            })
        # Sort by total trades desc
        rows.sort(key=lambda x: -x["total"])
        return rows

    def summary(self) -> dict:
        all_wins = sum(s["wins"] for s in self._stats.values())
        all_losses = sum(s["losses"] for s in self._stats.values())
        total = all_wins + all_losses
        total_pnl = round(sum(s["pnl"] for s in self._stats.values()), 4)
        return {
            "total_trades": total,
            "wins": all_wins,
            "losses": all_losses,
            "win_rate_pct": round(all_wins / total * 100, 1) if total else 0.0,
            "total_pnl": total_pnl,
        }


class StructuredLogger:
    def __init__(self, config: dict):
        log_cfg = config.get("logging", {})
        self._trade_path = Path(log_cfg.get("trade_log_file", "logs/trades.jsonl"))
        self._signal_path = Path(log_cfg.get("signal_log_file", "logs/signals.jsonl"))
        self._metrics_path = Path(log_cfg.get("metrics_log_file", "logs/metrics.jsonl"))

        for p in (self._trade_path, self._signal_path, self._metrics_path):
            p.parent.mkdir(exist_ok=True)

        self.win_rate = WinRateTracker(self._trade_path)

    def log_signal(self, signal: SignalResult):
        # Only log signals that were considered (not silent rejections)
        if signal.yes_price >= 0.97:
            self._write(self._signal_path, signal.to_dict())

    @staticmethod
    def _bot(pos: Any) -> str:
        return "bot2" if getattr(pos.market, "timeframe", "") == "1h" else "bot1"

    def log_trade_open(self, pos: Any):
        trade_side = getattr(pos, "trade_side", "yes")
        self._write(self._trade_path, {
            "event": "open",
            "order_id": pos.order.order_id,
            "market_id": pos.market.market_id,
            "coin": pos.market.coin,
            "timeframe": pos.market.timeframe,
            "mode": pos.mode,
            "side": trade_side,
            "entry_price": pos.entry_price,
            "entry_usdc": pos.entry_usdc,
            "timestamp": time.time(),
        })
        if _tdb:
            try:
                _tdb.log_trade(
                    bot=self._bot(pos), coin=pos.market.coin,
                    timeframe=pos.market.timeframe, mode=pos.mode,
                    side=trade_side,
                    entry_price=pos.entry_price, entry_usdc=pos.entry_usdc,
                    market_id=pos.market.market_id, order_id=pos.order.order_id,
                )
            except Exception as e:
                logger.warning(f"trade_db open write failed: {e}")

    def log_trade_closed(self, pos: Any):
        win = (pos.pnl or 0) > 0
        self._write(self._trade_path, {
            "event": "closed",
            "order_id": pos.order.order_id,
            "market_id": pos.market.market_id,
            "coin": pos.market.coin,
            "timeframe": pos.market.timeframe,
            "mode": pos.mode,
            "entry_price": pos.entry_price,
            "entry_usdc": pos.entry_usdc,
            "pnl": pos.pnl,
            "win": win,
            "redeemed": pos.redeemed,
            "timestamp": time.time(),
        })
        self.win_rate.record_closed(pos)
        if _tdb:
            try:
                _tdb.log_trade(
                    bot=self._bot(pos), coin=pos.market.coin,
                    timeframe=pos.market.timeframe, mode=pos.mode,
                    entry_price=pos.entry_price, entry_usdc=pos.entry_usdc,
                    pnl=pos.pnl, win=win,
                    market_id=pos.market.market_id, order_id=pos.order.order_id,
                )
                logger.info(f"trade_db: closed trade written coin={pos.market.coin} win={win} pnl={pos.pnl}")
            except Exception as e:
                logger.warning(f"trade_db closed write failed: {e}")

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
