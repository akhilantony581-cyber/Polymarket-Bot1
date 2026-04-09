"""
trade_db.py
Persistent trade log using Neon (or any PostgreSQL) + SQLite fallback.

Set DATABASE_URL in Railway env vars to enable persistence.
Falls back to logs/trades.db (SQLite) if DATABASE_URL is not set.
"""

import json
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
_USE_PG = bool(DATABASE_URL)
_pg_pool = None

_DATA_DIR = Path(os.environ.get("DATA_DIR", "logs"))
_DB_PATH  = _DATA_DIR / "trades.db"


# ── PostgreSQL ────────────────────────────────────────────────────────────────

def _pg():
    global _pg_pool
    if _pg_pool is None:
        import psycopg2
        from psycopg2 import pool as _pool
        _pg_pool = _pool.ThreadedConnectionPool(
            1, 5, DATABASE_URL, connect_timeout=5
        )
    return _pg_pool.getconn()

def _pg_release(conn):
    if _pg_pool:
        _pg_pool.putconn(conn)


# ── SQLite ────────────────────────────────────────────────────────────────────

def _sqlite():
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(_DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c


# ── Init ──────────────────────────────────────────────────────────────────────

def init_db():
    global _USE_PG
    if _USE_PG:
        try:
            conn = _pg()
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trades (
                        id          SERIAL PRIMARY KEY,
                        timestamp   DOUBLE PRECISION NOT NULL,
                        bot         TEXT NOT NULL,
                        coin        TEXT NOT NULL,
                        timeframe   TEXT NOT NULL,
                        mode        TEXT NOT NULL,
                        side        TEXT NOT NULL DEFAULT 'yes',
                        entry_price DOUBLE PRECISION NOT NULL,
                        entry_usdc  DOUBLE PRECISION NOT NULL,
                        pnl         DOUBLE PRECISION,
                        win         SMALLINT,
                        market_id   TEXT DEFAULT '',
                        order_id    TEXT DEFAULT ''
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS analysis_cache (
                        id           SERIAL PRIMARY KEY,
                        generated_at DOUBLE PRECISION NOT NULL,
                        analysis     TEXT NOT NULL,
                        stats_json   TEXT NOT NULL
                    )
                """)
                # unique index on order_id (ignore empty strings)
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_order_id
                    ON trades(order_id) WHERE order_id <> ''
                """)
            conn.commit()
            _pg_release(conn)
            logger.info("TradeDB (PostgreSQL/Neon) initialised")
            return
        except Exception as e:
            logger.error(f"TradeDB PG failed, falling back to SQLite: {e}")
            _USE_PG = False

    # SQLite fallback
    with _sqlite() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   REAL NOT NULL,
                bot         TEXT NOT NULL,
                coin        TEXT NOT NULL,
                timeframe   TEXT NOT NULL,
                mode        TEXT NOT NULL,
                side        TEXT NOT NULL DEFAULT 'yes',
                entry_price REAL NOT NULL,
                entry_usdc  REAL NOT NULL,
                pnl         REAL,
                win         INTEGER,
                market_id   TEXT DEFAULT '',
                order_id    TEXT DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS analysis_cache (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                generated_at REAL NOT NULL,
                analysis     TEXT NOT NULL,
                stats_json   TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trades_ts  ON trades(timestamp);
            CREATE INDEX IF NOT EXISTS idx_trades_bot ON trades(bot);
        """)
    logger.info(f"TradeDB (SQLite) initialised at {_DB_PATH}")


def clear_all_trades():
    """Delete all rows from trades and analysis_cache tables."""
    if _USE_PG:
        conn = _pg()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM trades")
                cur.execute("DELETE FROM analysis_cache")
            conn.commit()
        finally:
            _pg_release(conn)
    else:
        with _sqlite() as c:
            c.execute("DELETE FROM trades")
            c.execute("DELETE FROM analysis_cache")
    logger.info("TradeDB: all trades cleared")


# ── Write ─────────────────────────────────────────────────────────────────────

def log_trade(*, bot, coin, timeframe, mode, side="yes", entry_price,
              entry_usdc, pnl=None, win=None, market_id="", order_id=""):
    win_int = int(win) if win is not None else None
    ph = "%s" if _USE_PG else "?"

    if _USE_PG:
        conn = _pg()
        try:
            with conn.cursor() as cur:
                if order_id:
                    cur.execute(f"SELECT id FROM trades WHERE order_id={ph}", (order_id,))
                    row = cur.fetchone()
                    if row:
                        cur.execute(
                            f"UPDATE trades SET pnl={ph}, win={ph} WHERE order_id={ph}",
                            (pnl, win_int, order_id)
                        )
                        conn.commit()
                        return
                cur.execute(f"""
                    INSERT INTO trades
                    (timestamp,bot,coin,timeframe,mode,side,entry_price,entry_usdc,pnl,win,market_id,order_id)
                    VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
                """, (time.time(), bot, coin, timeframe, mode, side,
                      entry_price, entry_usdc, pnl, win_int, market_id, order_id))
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.warning(f"trade_db write error: {e}")
        finally:
            _pg_release(conn)
    else:
        with _sqlite() as c:
            if order_id:
                row = c.execute("SELECT id FROM trades WHERE order_id=?", (order_id,)).fetchone()
                if row:
                    c.execute("UPDATE trades SET pnl=?, win=? WHERE order_id=?", (pnl, win_int, order_id))
                    return
            c.execute("""
                INSERT OR IGNORE INTO trades
                (timestamp,bot,coin,timeframe,mode,side,entry_price,entry_usdc,pnl,win,market_id,order_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (time.time(), bot, coin, timeframe, mode, side,
                  entry_price, entry_usdc, pnl, win_int, market_id, order_id))


# ── Read ──────────────────────────────────────────────────────────────────────

def get_trades(limit=500, bot=None, resolved_only=False) -> List[dict]:
    ph = "%s" if _USE_PG else "?"
    where, params = [], []
    if bot:
        where.append(f"bot={ph}"); params.append(bot)
    if resolved_only:
        where.append("win IS NOT NULL")
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"SELECT * FROM trades {clause} ORDER BY timestamp DESC LIMIT {ph}"
    params.append(limit)

    if _USE_PG:
        conn = _pg()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            _pg_release(conn)
    else:
        with _sqlite() as c:
            return [dict(r) for r in c.execute(sql, params).fetchall()]


def get_stats() -> dict:
    agg = """
        SELECT bot, coin, timeframe, mode,
               COUNT(*) AS total,
               COALESCE(SUM(win),0) AS wins,
               COALESCE(SUM(CASE WHEN win=0 THEN 1 ELSE 0 END),0) AS losses,
               ROUND((AVG(win::NUMERIC)*100)::NUMERIC,1) AS win_rate,
               ROUND(SUM(COALESCE(pnl,0))::NUMERIC,4) AS total_pnl
        FROM trades WHERE win IS NOT NULL
        GROUP BY bot,coin,timeframe,mode ORDER BY bot,total DESC
    """
    summary = """
        SELECT COUNT(*) AS total,
               COALESCE(SUM(win),0) AS wins,
               COALESCE(SUM(CASE WHEN win=0 THEN 1 ELSE 0 END),0) AS losses,
               ROUND((AVG(win::NUMERIC)*100)::NUMERIC,1) AS win_rate,
               ROUND(SUM(COALESCE(pnl,0))::NUMERIC,4) AS total_pnl
        FROM trades WHERE win IS NOT NULL
    """
    bot_q = "SELECT COUNT(*) AS total, COALESCE(SUM(win),0) AS wins, ROUND(SUM(COALESCE(pnl,0))::NUMERIC,4) AS pnl FROM trades WHERE bot={ph} AND win IS NOT NULL"

    if _USE_PG:
        conn = _pg()
        try:
            def q(sql, params=()):
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    cols = [d[0] for d in cur.description]
                    rows = cur.fetchall()
                    return dict(zip(cols, rows[0])) if len(rows)==1 else [dict(zip(cols,r)) for r in rows]
            return {
                "summary": q(summary),
                "bot1": q(bot_q.replace("{ph}", "%s"), ("bot1",)),
                "bot2": q(bot_q.replace("{ph}", "%s"), ("bot2",)),
                "rows": q(agg),
            }
        finally:
            _pg_release(conn)
    else:
        with _sqlite() as c:
            def q(sql, params=()):
                rows = c.execute(sql, params).fetchall()
                return dict(rows[0]) if len(rows)==1 else [dict(r) for r in rows]
            return {
                "summary": q(summary),
                "bot1": q(bot_q.replace("{ph}", "?"), ("bot1",)),
                "bot2": q(bot_q.replace("{ph}", "?"), ("bot2",)),
                "rows": q(agg),
            }


# ── Bot performance summary ───────────────────────────────────────────────────

def get_bot_summary() -> dict:
    """
    Returns per-bot performance aggregates, with bot1 broken down by mode
    (sniper = Snipe 1, snipe2 = Snipe 2).

    Shape:
    {
      "bot1": {
        "snipe1":  {trades, volume, profit, wins, losses, win_rate},
        "snipe2":  {trades, volume, profit, wins, losses, win_rate},
        "total":   {trades, volume, profit, wins, losses, win_rate},
      },
      "bot2": {trades, volume, profit, wins, losses, win_rate},
      "bot3": {trades, volume, profit, wins, losses, win_rate},
    }
    """
    sql = """
        SELECT bot, mode,
               COUNT(*)                                          AS trades,
               COALESCE(SUM(entry_usdc), 0)                     AS volume,
               COALESCE(SUM(win), 0)                            AS wins,
               COALESCE(SUM(CASE WHEN win=0 THEN 1 ELSE 0 END),0) AS losses,
               COALESCE(SUM(pnl), 0)                            AS profit
        FROM trades
        GROUP BY bot, mode
        ORDER BY bot, mode
    """
    if _USE_PG:
        conn = _pg()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            _pg_release(conn)
    else:
        with _sqlite() as c:
            rows = [dict(r) for r in c.execute(sql).fetchall()]

    def _row(trades, volume, wins, losses, profit):
        resolved = wins + losses
        return {
            "trades":   int(trades),
            "volume":   round(float(volume), 2),
            "profit":   round(float(profit), 4),
            "wins":     int(wins),
            "losses":   int(losses),
            "win_rate": round(wins / resolved * 100, 1) if resolved else 0.0,
        }

    # Aggregate into the shape callers expect
    agg: dict = {"bot1": {}, "bot2": {}, "bot3": {}}
    b1_buckets: dict = {}

    for r in rows:
        bot  = r["bot"]
        mode = r["mode"]
        d    = _row(r["trades"], r["volume"], r["wins"], r["losses"], r["profit"])
        if bot == "bot1":
            key = "snipe2" if mode == "snipe2" else "snipe1"
            b1_buckets[key] = d
        elif bot in agg:
            agg[bot] = d

    # Bot 1 total
    all_b1 = b1_buckets.values()
    agg["bot1"] = {
        "snipe1": b1_buckets.get("snipe1", _row(0,0,0,0,0)),
        "snipe2": b1_buckets.get("snipe2", _row(0,0,0,0,0)),
        "total": _row(
            sum(x["trades"] for x in all_b1),
            sum(x["volume"] for x in all_b1),
            sum(x["wins"]   for x in all_b1),
            sum(x["losses"] for x in all_b1),
            sum(x["profit"] for x in all_b1),
        ) if b1_buckets else _row(0,0,0,0,0),
    }
    for k in ("bot2", "bot3"):
        if not agg[k]:
            agg[k] = _row(0,0,0,0,0)

    return agg


# ── Analysis cache ────────────────────────────────────────────────────────────

def save_analysis(analysis: str, stats: dict):
    stats_json = json.dumps(stats)
    if _USE_PG:
        conn = _pg()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM analysis_cache")
                cur.execute(
                    "INSERT INTO analysis_cache (generated_at,analysis,stats_json) VALUES (%s,%s,%s)",
                    (time.time(), analysis, stats_json)
                )
            conn.commit()
        finally:
            _pg_release(conn)
    else:
        with _sqlite() as c:
            c.execute("DELETE FROM analysis_cache")
            c.execute("INSERT INTO analysis_cache (generated_at,analysis,stats_json) VALUES (?,?,?)",
                      (time.time(), analysis, stats_json))


def load_analysis() -> Optional[dict]:
    sql = "SELECT * FROM analysis_cache ORDER BY generated_at DESC LIMIT 1"
    if _USE_PG:
        conn = _pg()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                if not row: return None
                cols = [d[0] for d in cur.description]
                d = dict(zip(cols, row))
                return {"analysis": d["analysis"], "stats": json.loads(d["stats_json"]), "generated_at": d["generated_at"]}
        finally:
            _pg_release(conn)
    else:
        with _sqlite() as c:
            row = c.execute(sql).fetchone()
            if not row: return None
            return {"analysis": row["analysis"], "stats": json.loads(row["stats_json"]), "generated_at": row["generated_at"]}


# ── Migrate existing trades.jsonl ─────────────────────────────────────────────

def hydrate_from_jsonl(jsonl_path: Path):
    if not jsonl_path.exists(): return
    imported = 0
    try:
        with open(jsonl_path) as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try: rec = json.loads(line)
                except: continue
                if rec.get("event") != "closed": continue
                tf  = rec.get("timeframe", "15m")
                pnl = rec.get("pnl")
                win = rec.get("win")
                if win is None and pnl is not None: win = pnl > 0
                log_trade(
                    bot="bot2" if tf=="1h" else "bot1",
                    coin=rec.get("coin","?"), timeframe=tf,
                    mode=rec.get("mode","?"), side=rec.get("side","yes"),
                    entry_price=rec.get("entry_price",0.0),
                    entry_usdc=rec.get("entry_usdc",0.0),
                    pnl=pnl, win=win,
                    market_id=rec.get("market_id",""),
                    order_id=rec.get("order_id",""),
                )
                imported += 1
    except Exception as e:
        logger.warning(f"hydrate_from_jsonl error: {e}")
    if imported:
        logger.info(f"TradeDB: migrated {imported} trades from {jsonl_path}")
