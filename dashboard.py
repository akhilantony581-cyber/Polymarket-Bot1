"""
dashboard.py
FastAPI dashboard server.
Runs alongside the trading bot, reads bot state, and serves live controls.
Start with: python dashboard.py (alongside main.py)
"""

import asyncio
import builtins
import json
import logging
import os
import time
from pathlib import Path
import yaml
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

logger = logging.getLogger(__name__)
app = FastAPI(title="Polymarket Bot Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CONFIG_PATH = "config.yaml"


def get_bot():
    return getattr(builtins, "_bot", None)


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def save_config(config: dict):
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


# ------------------------------------------------------------------
# MODELS
# ------------------------------------------------------------------
class PriceRangeUpdate(BaseModel):
    min_entry: float
    sniper_min: float


class BudgetUpdate(BaseModel):
    total: float
    max_concurrent: int


class TradeUpdate(BaseModel):
    kelly_score_95: float
    kelly_score_90: float
    kelly_score_85: float


class ManualExitRequest(BaseModel):
    order_id: str
    exit_price: float


# ------------------------------------------------------------------
# DASHBOARD HTML
# ------------------------------------------------------------------
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Polymarket Bot</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0d1117; color: #e6edf3; font-family: 'Courier New', monospace; font-size: 13px; }
  .header { background: #161b22; padding: 16px 24px; border-bottom: 1px solid #30363d; display: flex; align-items: center; gap: 16px; }
  .header h1 { font-size: 18px; color: #58a6ff; }
  .status-dot { width: 10px; height: 10px; border-radius: 50%; background: #3fb950; display: inline-block; }
  .status-dot.paused { background: #e3b341; }
  .status-dot.halted { background: #f85149; }
  .grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; padding: 16px; }
  .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; padding: 0 16px 16px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
  .card h3 { color: #8b949e; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 12px; }
  .metric-value { font-size: 28px; font-weight: bold; color: #e6edf3; }
  .metric-value.positive { color: #3fb950; }
  .metric-value.negative { color: #f85149; }
  .metric-sub { font-size: 11px; color: #8b949e; margin-top: 4px; }
  .control-row { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; }
  .control-row label { color: #8b949e; width: 160px; font-size: 12px; }
  input[type=number], input[type=range] { background: #0d1117; border: 1px solid #30363d; color: #e6edf3; padding: 6px 10px; border-radius: 4px; width: 120px; font-family: monospace; }
  input[type=range] { width: 160px; padding: 0; }
  .range-val { color: #58a6ff; width: 60px; }
  button { padding: 8px 16px; border-radius: 4px; border: none; cursor: pointer; font-family: monospace; font-size: 12px; font-weight: bold; }
  .btn-pause { background: #e3b341; color: #0d1117; }
  .btn-resume { background: #3fb950; color: #0d1117; }
  .btn-halt { background: #f85149; color: #fff; }
  .btn-reset { background: #58a6ff; color: #0d1117; }
  .btn-save { background: #3fb950; color: #0d1117; }
  .btn-exit { background: #f85149; color: #fff; font-size: 11px; padding: 4px 10px; }
  .btn-row { display: flex; gap: 8px; margin-top: 12px; }
  table { width: 100%; border-collapse: collapse; }
  th { color: #8b949e; font-size: 11px; text-transform: uppercase; padding: 8px 6px; border-bottom: 1px solid #30363d; text-align: left; }
  td { padding: 8px 6px; border-bottom: 1px solid #21262d; font-size: 12px; }
  tr:last-child td { border-bottom: none; }
  .tag { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 10px; font-weight: bold; }
  .tag-standard { background: #1f6feb22; color: #58a6ff; border: 1px solid #1f6feb; }
  .tag-sniper { background: #f8514922; color: #f85149; border: 1px solid #f85149; }
  .tag-maker { background: #3fb95022; color: #3fb950; border: 1px solid #3fb950; }
  .win { color: #3fb950; } .loss { color: #f85149; }
  #log { height: 160px; overflow-y: auto; background: #0d1117; border: 1px solid #30363d; border-radius: 4px; padding: 8px; font-size: 11px; }
  .log-line { margin-bottom: 3px; }
  .log-line.info { color: #8b949e; }
  .log-line.trade { color: #3fb950; }
  .log-line.cancel { color: #e3b341; }
  .log-line.error { color: #f85149; }
  .section-pad { padding: 0 16px 16px; }
  .divider { border: none; border-top: 1px solid #21262d; margin: 0 16px 16px; }
</style>
</head>
<body>

<div class="header">
  <span class="status-dot" id="statusDot"></span>
  <h1>Polymarket Trading Bot</h1>
  <span id="statusText" style="color:#8b949e; font-size:12px;"></span>
  <span style="margin-left:auto; color:#8b949e; font-size:11px;" id="lastUpdate"></span>
</div>

<!-- Metrics Row -->
<div class="grid">
  <div class="card">
    <h3>Total P&amp;L</h3>
    <div class="metric-value" id="totalPnl">$0.00</div>
    <div class="metric-sub" id="winRate">Win rate: 0%</div>
  </div>
  <div class="card">
    <h3>Trades Today</h3>
    <div class="metric-value" id="totalTrades">0</div>
    <div class="metric-sub" id="winsLosses">0W / 0L</div>
  </div>
  <div class="card">
    <h3>Capital Deployed</h3>
    <div class="metric-value" id="capitalDeployed">$0.00</div>
    <div class="metric-sub" id="capitalTotal">of $100.00</div>
  </div>
</div>

<!-- Controls -->
<div class="grid-2">
  <div class="card">
    <h3>Bot Controls</h3>
    <div class="btn-row">
      <button class="btn-pause" onclick="pauseBot()">⏸ Pause New Trades</button>
      <button class="btn-resume" onclick="resumeBot()">▶ Resume</button>
    </div>
    <div class="btn-row" style="margin-top:8px">
      <button class="btn-halt" onclick="haltBot()">🛑 Emergency Halt</button>
      <button class="btn-reset" onclick="resetHalt()">↺ Reset Halt</button>
    </div>
  </div>

  <div class="card">
    <h3>Price Settings</h3>
    <div class="control-row">
      <label>Min Entry Price</label>
      <input type="range" min="0.97" max="0.995" step="0.001" id="minEntry" oninput="document.getElementById('minEntryVal').textContent=parseFloat(this.value).toFixed(3)">
      <span class="range-val" id="minEntryVal">0.980</span>
    </div>
    <div class="control-row">
      <label>Sniper Threshold</label>
      <input type="range" min="0.985" max="0.999" step="0.001" id="sniperMin" oninput="document.getElementById('sniperMinVal').textContent=parseFloat(this.value).toFixed(3)">
      <span class="range-val" id="sniperMinVal">0.990</span>
    </div>
    <button class="btn-save" onclick="savePriceSettings()" style="margin-top:8px">Save Price Settings</button>
  </div>
</div>

<div class="grid-2" style="padding-top:0">
  <div class="card">
    <h3>Budget Settings</h3>
    <div class="control-row">
      <label>Total Capital ($)</label>
      <input type="number" id="totalCapital" min="10" step="10" value="100">
    </div>
    <div class="control-row">
      <label>Max Concurrent Trades</label>
      <input type="number" id="maxConcurrent" min="1" max="5" step="1" value="2">
    </div>
    <button class="btn-save" onclick="saveBudget()" style="margin-top:8px">Save Budget</button>
  </div>

  <div class="card">
    <h3>Kelly Position Sizing</h3>
    <div class="control-row">
      <label>Score 95-100 (%)</label>
      <input type="number" id="kelly95" min="1" max="50" step="1" value="15">
    </div>
    <div class="control-row">
      <label>Score 90-94 (%)</label>
      <input type="number" id="kelly90" min="1" max="30" step="1" value="10">
    </div>
    <div class="control-row">
      <label>Score 85-89 (%)</label>
      <input type="number" id="kelly85" min="1" max="20" step="1" value="6">
    </div>
    <button class="btn-save" onclick="saveKelly()" style="margin-top:8px">Save Kelly</button>
  </div>
</div>

<hr class="divider">

<!-- Active Orders -->
<div class="section-pad">
  <div class="card">
    <h3>Active Orders</h3>
    <table>
      <thead><tr><th>Market</th><th>Coin</th><th>Mode</th><th>Price</th><th>Size</th><th>Age</th><th>Action</th></tr></thead>
      <tbody id="activeOrders"><tr><td colspan="7" style="color:#8b949e;text-align:center;padding:16px">No active orders</td></tr></tbody>
    </table>
  </div>
</div>

<!-- Open Positions -->
<div class="section-pad">
  <div class="card">
    <h3>Open Positions (Filled)</h3>
    <table>
      <thead><tr><th>Market</th><th>Coin</th><th>Mode</th><th>Entry</th><th>Size</th><th>Status</th><th>PnL</th><th>Exit</th></tr></thead>
      <tbody id="positions"><tr><td colspan="8" style="color:#8b949e;text-align:center;padding:16px">No open positions</td></tr></tbody>
    </table>
  </div>
</div>

<!-- Recent Trades -->
<div class="section-pad">
  <div class="card">
    <h3>Recent Trades</h3>
    <table>
      <thead><tr><th>Time</th><th>Market</th><th>Coin</th><th>Mode</th><th>Entry</th><th>Size</th><th>PnL</th><th>Result</th></tr></thead>
      <tbody id="recentTrades"><tr><td colspan="8" style="color:#8b949e;text-align:center;padding:16px">No trades yet</td></tr></tbody>
    </table>
  </div>
</div>

<!-- Live Log -->
<div class="section-pad">
  <div class="card">
    <h3>Live Log</h3>
    <div id="log"></div>
  </div>
</div>

<script>
let ws;
let state = {};

function connect() {
  ws = new WebSocket(`ws://${location.host}/ws`);
  ws.onmessage = e => {
    const data = JSON.parse(e.data);
    if (data.type === 'state') updateState(data.payload);
    if (data.type === 'log') appendLog(data.payload);
  };
  ws.onclose = () => setTimeout(connect, 2000);
}

function updateState(s) {
  state = s;
  const metrics = s.metrics || {};

  // Status
  const dot = document.getElementById('statusDot');
  const txt = document.getElementById('statusText');
  dot.className = 'status-dot' + (s.halted ? ' halted' : s.paused ? ' paused' : '');
  txt.textContent = s.halted ? 'HALTED' : s.paused ? 'PAUSED — No new trades' : 'RUNNING';

  // Metrics
  const pnl = metrics.total_pnl || 0;
  document.getElementById('totalPnl').textContent = `${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)}`;
  document.getElementById('totalPnl').className = 'metric-value ' + (pnl >= 0 ? 'positive' : 'negative');
  document.getElementById('winRate').textContent = `Win rate: ${metrics.win_rate || 0}%`;
  document.getElementById('totalTrades').textContent = metrics.total_trades || 0;
  document.getElementById('winsLosses').textContent = `${metrics.wins || 0}W / ${metrics.losses || 0}L`;
  document.getElementById('capitalDeployed').textContent = `$${(metrics.capital_deployed || 0).toFixed(2)}`;
  document.getElementById('capitalTotal').textContent = `of $${(s.config?.total_capital || 100).toFixed(2)}`;
  document.getElementById('lastUpdate').textContent = new Date().toLocaleTimeString();

  // Config sliders
  if (s.config) {
    const me = document.getElementById('minEntry');
    me.value = s.config.min_entry;
    document.getElementById('minEntryVal').textContent = parseFloat(s.config.min_entry).toFixed(3);
    document.getElementById('totalCapital').value = s.config.total_capital;
    document.getElementById('maxConcurrent').value = s.config.max_concurrent;
  }

  // Active Orders
  const aoTbody = document.getElementById('activeOrders');
  if (!s.active_orders || s.active_orders.length === 0) {
    aoTbody.innerHTML = '<tr><td colspan="7" style="color:#8b949e;text-align:center;padding:16px">No active orders</td></tr>';
  } else {
    aoTbody.innerHTML = s.active_orders.map(o => `
      <tr>
        <td style="font-size:11px;max-width:160px;overflow:hidden;text-overflow:ellipsis">${o.market_id.substring(0,20)}...</td>
        <td>${o.coin}</td>
        <td><span class="tag tag-${o.mode}">${o.mode}</span></td>
        <td>${o.price.toFixed(4)}</td>
        <td>$${o.size.toFixed(2)}</td>
        <td>${o.age}s</td>
        <td>—</td>
      </tr>`).join('');
  }

  // Positions
  const posTbody = document.getElementById('positions');
  if (!s.positions || s.positions.length === 0) {
    posTbody.innerHTML = '<tr><td colspan="8" style="color:#8b949e;text-align:center;padding:16px">No open positions</td></tr>';
  } else {
    posTbody.innerHTML = s.positions.map(p => `
      <tr>
        <td style="font-size:11px">${p.market_id.substring(0,20)}...</td>
        <td>${p.coin}</td>
        <td><span class="tag tag-${p.mode}">${p.mode}</span></td>
        <td>${p.entry_price.toFixed(4)}</td>
        <td>$${p.size.toFixed(2)}</td>
        <td>${p.redeemed ? '<span class="win">Redeemed</span>' : 'Holding'}</td>
        <td class="${p.pnl >= 0 ? 'win' : 'loss'}">${p.pnl !== null ? (p.pnl >= 0 ? '+' : '') + p.pnl.toFixed(4) : '—'}</td>
        <td>${!p.redeemed ? `<button class="btn-exit" onclick="promptExit('${p.order_id}', ${p.entry_price})">Exit</button>` : '—'}</td>
      </tr>`).join('');
  }

  // Recent Trades
  const rtTbody = document.getElementById('recentTrades');
  if (!s.recent_trades || s.recent_trades.length === 0) {
    rtTbody.innerHTML = '<tr><td colspan="8" style="color:#8b949e;text-align:center;padding:16px">No trades yet</td></tr>';
  } else {
    rtTbody.innerHTML = [...s.recent_trades].reverse().slice(0,15).map(t => `
      <tr>
        <td>${new Date(t.timestamp * 1000).toLocaleTimeString()}</td>
        <td style="font-size:11px">${(t.market_id||'').substring(0,18)}...</td>
        <td>${t.coin}</td>
        <td><span class="tag tag-${t.mode}">${t.mode}</span></td>
        <td>${t.entry_price.toFixed(4)}</td>
        <td>$${t.entry_usdc.toFixed(2)}</td>
        <td class="${t.pnl >= 0 ? 'win' : 'loss'}">${t.pnl !== null ? (t.pnl >= 0 ? '+' : '') + t.pnl.toFixed(4) : '—'}</td>
        <td class="${t.win ? 'win' : 'loss'}">${t.win ? '✓ WIN' : '✗ LOSS'}</td>
      </tr>`).join('');
  }
}

function appendLog(line) {
  const log = document.getElementById('log');
  const cls = line.includes('filled') || line.includes('WIN') ? 'trade' :
              line.includes('cancel') ? 'cancel' :
              line.includes('ERROR') ? 'error' : 'info';
  const div = document.createElement('div');
  div.className = `log-line ${cls}`;
  div.textContent = `[${new Date().toLocaleTimeString()}] ${line}`;
  log.appendChild(div);
  log.scrollTop = log.scrollHeight;
  if (log.children.length > 200) log.removeChild(log.firstChild);
}

async function api(path, body) {
  const r = await fetch(path, {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
  const d = await r.json();
  appendLog(d.message || JSON.stringify(d));
  return d;
}

function pauseBot() { api('/control/pause', {}); }
function resumeBot() { api('/control/resume', {}); }
function haltBot() { if(confirm('Emergency halt all trading?')) api('/control/halt', {}); }
function resetHalt() { api('/control/reset_halt', {}); }

function savePriceSettings() {
  api('/settings/price', {
    min_entry: parseFloat(document.getElementById('minEntry').value),
    sniper_min: parseFloat(document.getElementById('sniperMin').value),
  });
}

function saveBudget() {
  api('/settings/budget', {
    total: parseFloat(document.getElementById('totalCapital').value),
    max_concurrent: parseInt(document.getElementById('maxConcurrent').value),
  });
}

function saveKelly() {
  api('/settings/trade', {
    kelly_score_95: parseFloat(document.getElementById('kelly95').value) / 100,
    kelly_score_90: parseFloat(document.getElementById('kelly90').value) / 100,
    kelly_score_85: parseFloat(document.getElementById('kelly85').value) / 100,
  });
}

function promptExit(orderId, entryPrice) {
  const price = prompt(`Exit price for position ${orderId}?\n(Entry was ${entryPrice.toFixed(4)})`, (entryPrice - 0.005).toFixed(4));
  if (price) api('/positions/exit', {order_id: orderId, exit_price: parseFloat(price)});
}

connect();
// Poll state every 3s via HTTP as backup
setInterval(() => fetch('/state').then(r=>r.json()).then(d=>updateState(d)), 3000);
</script>
</body>
</html>
"""


# ------------------------------------------------------------------
# API ROUTES
# ------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_HTML


@app.get("/health")
async def health():
    bot = get_bot()
    def masked(val: str) -> str:
        return val[:6] + "..." + val[-4:] if val and len(val) > 10 else ("SET" if val else "MISSING")

    pk    = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
    ak    = os.environ.get("POLYMARKET_API_KEY", "")
    sec   = os.environ.get("POLYMARKET_API_SECRET", "")
    pw    = os.environ.get("POLYMARKET_API_PASSPHRASE", "")
    tg    = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    rpc   = os.environ.get("POLYGON_RPC_URL", "")

    wallet = ""
    if bot:
        wallet = getattr(bot.execution, "_wallet_address", "")

    return {
        "bot_running":          bot is not None,
        "wallet_address":       wallet or "not loaded",
        "POLYMARKET_PRIVATE_KEY":   masked(pk),
        "POLYMARKET_API_KEY":       masked(ak),
        "POLYMARKET_API_SECRET":    masked(sec),
        "POLYMARKET_API_PASSPHRASE": masked(pw),
        "TELEGRAM_BOT_TOKEN":       masked(tg),
        "POLYGON_RPC_URL":          rpc if rpc else "MISSING (using default)",
        "trading_enabled":      bool(pk and ak and sec and pw),
        "telegram_enabled":     bool(tg),
    }


@app.get("/state")
async def get_state():
    bot = get_bot()
    if not bot:
        return {"error": "Bot not running"}
    return bot.get_state()


@app.post("/control/pause")
async def pause():
    bot = get_bot()
    if not bot:
        raise HTTPException(503, "Bot not running")
    bot.risk_manager.pause()
    return {"message": "Bot paused — existing positions continue, no new trades"}


@app.post("/control/resume")
async def resume():
    bot = get_bot()
    if not bot:
        raise HTTPException(503, "Bot not running")
    bot.risk_manager.resume()
    return {"message": "Bot resumed"}


@app.post("/control/halt")
async def halt():
    bot = get_bot()
    if not bot:
        raise HTTPException(503, "Bot not running")
    bot.risk_manager.emergency_halt("dashboard_user")
    return {"message": "Emergency halt activated"}


@app.post("/control/reset_halt")
async def reset_halt():
    bot = get_bot()
    if not bot:
        raise HTTPException(503, "Bot not running")
    bot.risk_manager.reset_halt()
    return {"message": "Halt reset — bot can trade again"}


@app.post("/settings/price")
async def update_price(data: PriceRangeUpdate):
    if data.min_entry < 0.97:
        raise HTTPException(400, "min_entry cannot be below 0.97")
    config = load_config()
    config["price"]["min_entry"] = round(data.min_entry, 4)
    config["price"]["sniper_min"] = round(data.sniper_min, 4)
    save_config(config)
    return {"message": f"Price settings updated: min={data.min_entry} sniper={data.sniper_min}"}


@app.post("/settings/budget")
async def update_budget(data: BudgetUpdate):
    if data.total < 10:
        raise HTTPException(400, "Total capital must be >= $10")
    config = load_config()
    config["capital"]["total"] = data.total
    config["capital"]["max_concurrent_trades"] = data.max_concurrent
    save_config(config)
    return {"message": f"Budget updated: capital=${data.total} concurrent={data.max_concurrent}"}


@app.post("/settings/trade")
async def update_trade(data: TradeUpdate):
    config = load_config()
    config["risk"]["kelly"]["score_95_100"] = round(data.kelly_score_95, 4)
    config["risk"]["kelly"]["score_90_94"] = round(data.kelly_score_90, 4)
    config["risk"]["kelly"]["score_85_89"] = round(data.kelly_score_85, 4)
    save_config(config)
    return {"message": "Kelly sizing updated"}


@app.post("/positions/exit")
async def manual_exit(data: ManualExitRequest):
    bot = get_bot()
    if not bot:
        raise HTTPException(503, "Bot not running")
    if data.exit_price < 0.90:
        raise HTTPException(400, "Exit price too low")
    success = await bot.order_manager.manual_exit_position(
        order_id=data.order_id,
        exit_price=data.exit_price,
    )
    if success:
        return {"message": f"Manual exit order placed at {data.exit_price}"}
    raise HTTPException(404, "Position not found or exit failed")


# ------------------------------------------------------------------
# WEBSOCKET for live log + state push
# ------------------------------------------------------------------
active_ws: list[WebSocket] = []


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_ws.append(websocket)
    try:
        # Send initial state
        bot = get_bot()
        if bot:
            await websocket.send_text(json.dumps({
                "type": "state",
                "payload": bot.get_state()
            }))
        while True:
            await asyncio.sleep(2)
            if bot:
                await websocket.send_text(json.dumps({
                    "type": "state",
                    "payload": bot.get_state()
                }))
    except WebSocketDisconnect:
        active_ws.remove(websocket)
    except Exception:
        active_ws.discard(websocket) if hasattr(active_ws, 'discard') else None


async def broadcast_log(message: str):
    for ws in list(active_ws):
        try:
            await ws.send_text(json.dumps({"type": "log", "payload": message}))
        except Exception:
            active_ws.remove(ws)


# ------------------------------------------------------------------
# ENTRY POINT (runs alongside main.py)
# ------------------------------------------------------------------
if __name__ == "__main__":
    import yaml
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    dash_cfg = cfg.get("dashboard", {})
    uvicorn.run(
        "dashboard:app",
        host=dash_cfg.get("host", "0.0.0.0"),
        port=dash_cfg.get("port", 8080),
        log_level="warning",
    )
