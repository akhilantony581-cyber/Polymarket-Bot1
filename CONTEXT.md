# Polymarket Bot — Session Context

Read this file at the start of every new chat session before making any changes.

---

## Project Location
- Directory: `/home/user/Polymarket-Bot1`
- Git branch: `claude/polymarket-trading-bot-siKb1`
- Deployed on: **Railway**

---

## What This Bot Does

Two bots running in the same process (`main.py`):

| | Bot 1 (Sniper) | Bot 2 (Market) |
|---|---|---|
| Timeframe | 15m | 1h |
| Mode | Sniper — buys YES tokens in last 60s | Market — buys in last 25 min |
| Min price | 0.97 (snipe1), 0.95 (snipe2) | 0.89 |
| Max per trade | $20 | $10 |

Both bots share one wallet (Polygon / USDC.e).

---

## Key Files

| File | Purpose |
|---|---|
| `main.py` | Entry point. Runs both bots, starts dashboard |
| `dashboard.py` | FastAPI web UI (port 8080). WebSocket push every 2s |
| `config.yaml` | All tunable parameters. Dashboard can edit live |
| `execution_engine.py` | Places/cancels orders via CLOB API. Has `get_usdc_balance()` |
| `signal_engine.py` | Scores markets, decides YES/NO/SKIP |
| `structured_logger.py` | Writes JSONL logs + calls trade_db |
| `trade_db.py` | Persistent trade log — Neon PostgreSQL + SQLite fallback |
| `polymarket_listener.py` | WebSocket feed from Polymarket |
| `binance_feed.py` | Binance price feed (oracle) |
| `risk_manager.py` | Kelly sizing, consecutive loss guard |
| `order_manager.py` | Tracks open positions |

---

## Environment Variables (set in Railway)

| Variable | Value / Notes |
|---|---|
| `POLY_PRIVATE_KEY` | Wallet private key |
| `POLY_API_KEY` | Polymarket CLOB API key |
| `POLY_API_SECRET` | Polymarket CLOB API secret |
| `POLY_API_PASSPHRASE` | Polymarket CLOB passphrase |
| `POLYGON_RPC_URL` | Polygon RPC for USDC balance reads |
| `TELEGRAM_BOT_TOKEN` | Telegram alerts |
| `TELEGRAM_CHAT_ID` | Telegram chat target |
| `DATABASE_URL` | Neon PostgreSQL connection string (for trade persistence) |

---

## Dashboard Features
- URL: Railway public domain on port 8080
- Live state pushed via WebSocket every 2s, HTTP fallback every 3s
- **Shared Capital** section: shows live USDC wallet balance (Polygon RPC, 30s cache)
- **Bot 1 / Bot 2** tabs: show config params with live edit
- **Trade Log** tab: persistent trade history from Neon DB
- **AI Analysis** tab: strategy suggestions (6h cache, `/analyze` endpoint to refresh)
- Live log panel: scrollable box (280px height), HTTP noise filtered

---

## Persistent Trade Storage (trade_db.py)

- Auto-detects `DATABASE_URL` env var
- If set → uses **Neon PostgreSQL** (`psycopg2`, `connect_timeout=5`)
- If not set → falls back to **SQLite** at `logs/trades.db`
- On startup: `hydrate_from_jsonl()` migrates existing `logs/trades.jsonl` into DB
- Trades upserted by `order_id` (open → pending, close → fills pnl/win)
- Key functions: `init_db()`, `log_trade()`, `get_trades()`, `get_stats()`, `save_analysis()`, `load_analysis()`

---

## Dashboard API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/state` | GET | Full bot state (positions, config, balance) |
| `/config` | POST | Update config.yaml live |
| `/trades/log` | GET | Trade history from DB (`?bot=bot1&limit=200`) |
| `/trades/analysis` | GET | Latest cached AI analysis |
| `/analyze` | POST | Run fresh AI analysis (6h cache) |
| `/stats` | GET | Win rate stats from DB |
| `/logs` | GET | Last N lines of live log |
| `/ws` | WS | WebSocket state stream |

---

## Known Working State (as of last session)

- Railway logs show: `TradeDB (PostgreSQL/Neon) initialised`
- Dashboard loads correctly, WebSocket connects on page load
- Live log is scrollable, HTTP noise filtered
- Balance fetched from Polygon RPC with 5s timeout guard
- `psycopg2-binary>=2.9.9` is in `requirements.txt`

---

## Git Workflow

```bash
# Always develop on this branch:
git checkout claude/polymarket-trading-bot-siKb1

# Push:
git push -u origin claude/polymarket-trading-bot-siKb1
```

---

## Things NOT To Do

- Do not re-read entire conversation history unless absolutely necessary
- Do not add features beyond what is asked
- Do not add error handling for impossible scenarios
- Do not create new files unless strictly required
