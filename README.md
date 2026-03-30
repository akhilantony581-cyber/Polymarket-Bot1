# Polymarket Trading Bot

A production-grade certainty-execution bot for Polymarket crypto markets.
Trades only when real-world outcomes are already decided and reversal probability is near zero.

## Strategy

- **Markets:** BTC, ETH, SOL, XRP — 5-minute and 15-minute only
- **Min entry price:** 0.98 (hard floor, never below)
- **Orders:** Limit orders only, never market orders
- **Modes:** Standard (0.98–0.989) | Sniper (0.99+, ≤40s expiry) | Maker (passive 0.93–0.96)
- **Position sizing:** Kelly criterion (6–15% based on certainty score)
- **Capital recycling:** Auto-redeem on resolution to free capital instantly

## Architecture

```
main.py              — Bot entry point, trading loop, config hot-reload
binance_feed.py      — Binance WebSocket: price, momentum, volatility
polymarket_listener.py — Polymarket CLOB: markets, prices, order books
signal_engine.py     — Reversal risk score (0–100), mode selection
execution_engine.py  — CLOB order placement, cancellation, redemption
order_manager.py     — Full order lifecycle: pending → filled → redeemed
risk_manager.py      — Capital limits, Kelly sizing, halt logic, Telegram
structured_logger.py — JSONL trade/signal/metrics logging
dashboard.py         — FastAPI live dashboard + WebSocket
config.yaml          — All tunable parameters (hot-reloaded)
```

## Setup

### 1. Clone and install

```bash
git clone <repo>
cd polymarket-bot
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your Polymarket API keys and Telegram credentials
```

### 3. Get Polymarket API credentials

1. Go to [Polymarket](https://polymarket.com) and connect your wallet
2. Visit [CLOB API docs](https://docs.polymarket.com) to generate API keys
3. Fund your wallet with USDC on Polygon
4. Add a small amount of MATIC for gas (~$2–3 worth)

### 4. Run

```bash
# Start the bot
python main.py

# Start the dashboard (separate terminal)
python dashboard.py
```

Dashboard available at: `http://localhost:8080`

### 5. Docker (recommended for VPS)

```bash
docker build -t polymarket-bot .
docker run -d \
  --env-file .env \
  -p 8080:8080 \
  --restart unless-stopped \
  --name polymarket-bot \
  polymarket-bot
```

## Dashboard

Access at `http://your-server:8080`

Controls:
- **Pause New Trades** — stops new entries, existing positions continue to resolution
- **Emergency Halt** — stops everything immediately
- **Price Settings** — adjust min entry and sniper threshold live
- **Budget Settings** — adjust total capital and max concurrent trades live
- **Kelly Sizing** — adjust position size percentages per certainty tier
- **Manual Exit** — sell back any filled position at a custom limit price

## Risk Rules

| Rule | Value |
|------|-------|
| Min entry price | 0.98 (hard floor) |
| Max concurrent trades | 2 |
| Starting capital | $100 |
| Kelly tiers | 6% / 10% / 15% |
| Consecutive loss halt | 3 losses |
| Order timeout (standard) | 20 seconds |
| Order timeout (sniper) | 5 seconds |

## Logs

All logs written to `logs/` as JSONL:
- `trades.jsonl` — every trade open/close/cancel event
- `signals.jsonl` — every signal evaluation
- `metrics.jsonl` — periodic metrics snapshots
- `bot.log` — general application log

## Cloud Deployment

Recommended: any VPS with 1 vCPU / 1GB RAM (DigitalOcean, Hetzner, AWS t3.micro)

```bash
# Install Docker on Ubuntu
curl -fsSL https://get.docker.com | sh

# Run with auto-restart
docker run -d --env-file .env -p 8080:8080 --restart unless-stopped polymarket-bot
```

Secure the dashboard port with nginx + password or restrict to your IP.
