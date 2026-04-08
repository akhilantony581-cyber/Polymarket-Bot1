"""
dashboard.py
FastAPI dashboard server.
Runs alongside the trading bot, reads bot state, and serves live controls.
Start with: python dashboard.py (alongside main.py)
"""

import asyncio
import builtins
import collections
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

# Circular buffer of recent log lines — included in every state push
_log_buffer: collections.deque = collections.deque(maxlen=200)


class WSLogHandler(logging.Handler):
    """Captures log records into a circular buffer for the dashboard."""
    def emit(self, record):
        try:
            msg = self.format(record)
            _log_buffer.append({"t": time.strftime("%H:%M:%S"), "msg": msg})
        except Exception:
            pass


def install_log_handler():
    """Call once from main.py after the event loop is running."""
    handler = WSLogHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(handler)

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


class Bot1Update(BaseModel):
    sniper_min: float
    min_entry: float
    per_trade: float
    per_market: float
    window: int = 180


class Snipe1hUpdate(BaseModel):
    enabled: bool = True
    min_price: float
    per_trade: float
    per_market: float
    window: int = 1500


class TradeUpdate(BaseModel):
    kelly_score_95: float
    kelly_score_90: float
    kelly_score_85: float


class ManualExitRequest(BaseModel):
    order_id: str
    exit_price: float


class ManualTradeRequest(BaseModel):
    coin: str        # BTC / ETH / SOL / XRP
    direction: str   # up / down
    timeframe: str   # 5m / 15m
    price: float     # limit price (0.50 – 0.99)
    size: float      # USDC size


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
  #log { height: 220px; overflow-y: auto; background: #0d1117; border: 1px solid #30363d; border-radius: 4px; padding: 8px; font-size: 11px; }
  .tab-bar { display:flex; gap:8px; padding:0 16px 12px; }
  .tab-btn { padding:8px 20px; border-radius:6px; border:1px solid #30363d; background:#161b22; color:#8b949e; cursor:pointer; font-family:monospace; font-size:13px; font-weight:bold; }
  .tab-btn.active { background:#1f6feb22; border-color:#1f6feb; color:#58a6ff; }
  .tab-panel { display:none; } .tab-panel.active { display:block; }
  .log-line { margin-bottom: 3px; }
  .log-line.info { color: #8b949e; }
  .log-line.trade { color: #3fb950; }
  .log-line.cancel { color: #e3b341; }
  .log-line.error { color: #f85149; }
  .log-line.price { color: #58a6ff; }
  .log-line.redeem { color: #d2a8ff; }
  .log-tab { background:#21262d; border:1px solid #30363d; color:#8b949e; padding:4px 12px; border-radius:4px; cursor:pointer; font-size:11px; }
  .log-tab.active { background:#388bfd22; border-color:#388bfd; color:#58a6ff; }
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
      <button onclick="wakeBot()" style="background:#1f6feb;color:#fff;border:none;border-radius:6px;padding:8px 18px;cursor:pointer;font-weight:bold">⚡ Wake Bot</button>
    </div>
    <div class="btn-row" style="margin-top:8px">
      <button class="btn-halt" onclick="haltBot()">🛑 Emergency Halt</button>
      <button class="btn-reset" onclick="resetHalt()">↺ Reset Halt</button>
    </div>
  </div>

</div>

<div class="grid-2" style="padding-top:0">
  <div class="card">
    <h3>Manual Redeem</h3>
    <div class="control-row">
      <label>Condition ID</label>
      <input type="text" id="redeemConditionId" placeholder="0x..." style="background:#0d1117;border:1px solid #30363d;color:#e6edf3;padding:6px 10px;border-radius:4px;font-family:monospace;width:100%">
    </div>
    <button class="btn-save" onclick="manualRedeem()" style="margin-top:8px;background:#238636">Redeem Now</button>
    <button class="btn-save" onclick="redeemAll()" style="margin-top:8px;margin-left:8px;background:#b08800">Redeem All Positions</button>
    <div id="redeemResult" style="margin-top:8px;font-size:12px;color:#8b949e"></div>
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

<!-- Bot Selector Tabs -->
<div class="tab-bar">
  <button class="tab-btn active" onclick="switchBot('bot1')">🤖 Bot 1 — 15m Sniper</button>
  <button class="tab-btn" onclick="switchBot('bot2')">⏱ Bot 2 — 1h Market</button>
</div>

<!-- BOT 1: 15m Sniper Settings -->
<div id="bot1Panel" class="tab-panel active">
<div class="grid-2" style="padding-top:0">
  <div class="card">
    <h3>Bot 1 — 15m Sniper Settings</h3>
    <div class="control-row">
      <label>Min Price (Sniper)</label>
      <input type="number" id="b1SniperMin" min="0.80" max="0.999" step="0.001" value="0.97">
      <span style="color:#8b949e;font-size:11px" id="b1SniperMinVal"></span>
    </div>
    <div class="control-row">
      <label>Min Entry (Hard Floor)</label>
      <input type="number" id="b1MinEntry" min="0.80" max="0.999" step="0.001" value="0.97">
    </div>
    <div class="control-row">
      <label>Max Per Trade ($)</label>
      <input type="number" id="b1PerTrade" min="1" max="500" step="1" value="20">
    </div>
    <div class="control-row">
      <label>Max Per Market ($)</label>
      <input type="number" id="b1PerMarket" min="1" max="500" step="1" value="20">
    </div>
    <div class="control-row">
      <label>Trade Window (seconds)</label>
      <input type="number" id="b1Window" min="30" max="900" step="10" value="180">
    </div>
    <button class="btn-save" onclick="saveBot1()" style="margin-top:8px">💾 Save Bot 1 Settings</button>
    <div id="bot1Msg" style="margin-top:8px;font-size:12px;color:#3fb950"></div>
  </div>

  <div class="card">
    <h3>Bot 1 — Snipe 2 (last 10s FOK)</h3>
    <div class="control-row">
      <label>Enabled</label>
      <input type="checkbox" id="s2Enabled" checked style="width:auto">
    </div>
    <div class="control-row">
      <label>Min Price</label>
      <input type="number" id="s2MinPrice" min="0.80" max="0.99" step="0.01" value="0.95">
    </div>
    <div class="control-row">
      <label>Max Per Trade ($)</label>
      <input type="number" id="s2PerTrade" min="1" step="1" value="20">
    </div>
    <div class="control-row">
      <label>Max Per Market ($)</label>
      <input type="number" id="s2PerMarket" min="1" step="1" value="20">
    </div>
    <div class="control-row">
      <label>Window (seconds)</label>
      <input type="number" id="s2Window" min="5" max="30" step="1" value="10">
    </div>
    <button class="btn-save" onclick="saveSnipe2()" style="margin-top:8px">💾 Save Snipe 2</button>
    <div id="snipe2Msg" style="margin-top:8px;font-size:12px;color:#3fb950"></div>
  </div>
</div>
</div>

<!-- BOT 2: 1h Market Settings -->
<div id="bot2Panel" class="tab-panel">
<div class="grid-2" style="padding-top:0">
  <div class="card">
    <h3>Bot 2 — 1h Market Settings</h3>
    <div class="control-row">
      <label>Enabled</label>
      <input type="checkbox" id="b2Enabled" checked style="width:auto">
    </div>
    <div class="control-row">
      <label>Min Price</label>
      <input type="number" id="b2MinPrice" min="0.50" max="0.999" step="0.001" value="0.89">
    </div>
    <div class="control-row">
      <label>Max Per Trade ($)</label>
      <input type="number" id="b2PerTrade" min="1" max="500" step="1" value="10">
    </div>
    <div class="control-row">
      <label>Max Per Market ($)</label>
      <input type="number" id="b2PerMarket" min="1" max="500" step="1" value="50">
    </div>
    <div class="control-row">
      <label>Trade Window (seconds)</label>
      <input type="number" id="b2Window" min="60" max="7200" step="60" value="1500">
    </div>
    <button class="btn-save" onclick="saveBot2()" style="margin-top:8px">💾 Save Bot 2 Settings</button>
    <div id="bot2Msg" style="margin-top:8px;font-size:12px;color:#3fb950"></div>
  </div>

  <div class="card">
    <h3>Bot 2 — Current Parameters</h3>
    <table>
      <tbody>
        <tr><td style="color:#8b949e">Status</td><td id="b2Status">—</td></tr>
        <tr><td style="color:#8b949e">Min Price</td><td id="b2CurMinPrice">—</td></tr>
        <tr><td style="color:#8b949e">Per Trade</td><td id="b2CurPerTrade">—</td></tr>
        <tr><td style="color:#8b949e">Per Market</td><td id="b2CurPerMarket">—</td></tr>
        <tr><td style="color:#8b949e">Window</td><td id="b2CurWindow">—</td></tr>
        <tr><td style="color:#8b949e">1h Markets Tracked</td><td id="b2Markets">—</td></tr>
      </tbody>
    </table>
  </div>
</div>
</div>

<hr class="divider">

<!-- Shared Settings -->
<div class="grid-2" style="padding-top:0">
  <div class="card">
    <h3>Shared — Capital & Risk</h3>
    <div style="background:#0d1117;border:1px solid #30363d;border-radius:6px;padding:12px;margin-bottom:12px">
      <div style="font-size:11px;color:#8b949e;margin-bottom:4px">POLYMARKET WALLET BALANCE</div>
      <div id="cashBalance" style="font-size:26px;font-weight:bold;color:#3fb950">Loading...</div>
      <div style="font-size:11px;color:#8b949e;margin-top:4px">Live USDC.e on Polygon · refreshes every 30s</div>
    </div>
    <div class="control-row">
      <label>Bot Capital Budget ($)</label>
      <input type="number" id="totalCapital" min="10" step="10" value="100">
    </div>
    <div class="control-row">
      <label>Max Concurrent Trades</label>
      <input type="number" id="maxConcurrent" min="1" max="2000" step="1" value="500">
    </div>
    <button class="btn-save" onclick="saveBudget()" style="margin-top:8px">💾 Save Capital</button>
    <div id="budgetMsg" style="margin-top:8px;font-size:12px;color:#3fb950"></div>
  </div>

  <div class="card">
    <h3>Manual Redeem</h3>
    <div class="control-row">
      <label>Condition ID</label>
      <input type="text" id="redeemConditionId" placeholder="0x..." style="background:#0d1117;border:1px solid #30363d;color:#e6edf3;padding:6px 10px;border-radius:4px;font-family:monospace;width:100%">
    </div>
    <button class="btn-save" onclick="manualRedeem()" style="margin-top:8px;background:#238636">Redeem Now</button>
    <button class="btn-save" onclick="redeemAll()" style="margin-top:8px;margin-left:8px;background:#b08800">Redeem All</button>
    <div id="redeemResult" style="margin-top:8px;font-size:12px;color:#8b949e"></div>
  </div>
</div>

<hr class="divider">

<!-- Live Prices -->
<div class="grid-2" style="padding-top:0">
  <div class="card">
    <h3>Live Prices — Binance vs Polymarket</h3>
    <table>
      <thead><tr><th>Coin</th><th>Binance</th><th>PM Up Ask</th><th>PM Down Ask</th><th>Momentum</th></tr></thead>
      <tbody id="pricesTable">
        <tr><td colspan="5" style="color:#8b949e;text-align:center;padding:12px">Waiting for data...</td></tr>
      </tbody>
    </table>
  </div>

  <div class="card">
    <h3>Manual Trade</h3>
    <div class="control-row">
      <label>Coin</label>
      <select id="mCoin" style="background:#0d1117;border:1px solid #30363d;color:#e6edf3;padding:6px 10px;border-radius:4px;font-family:monospace">
        <option>BTC</option><option>ETH</option><option>SOL</option><option>XRP</option>
      </select>
    </div>
    <div class="control-row">
      <label>Timeframe</label>
      <select id="mTf" style="background:#0d1117;border:1px solid #30363d;color:#e6edf3;padding:6px 10px;border-radius:4px;font-family:monospace">
        <option>5m</option><option>15m</option>
      </select>
    </div>
    <div class="control-row">
      <label>Direction</label>
      <select id="mDir" style="background:#0d1117;border:1px solid #30363d;color:#e6edf3;padding:6px 10px;border-radius:4px;font-family:monospace">
        <option value="up">UP</option><option value="down">DOWN</option>
      </select>
    </div>
    <div class="control-row">
      <label>Limit Price</label>
      <input type="number" id="mPrice" min="0.50" max="0.99" step="0.01" value="0.60" style="width:100px">
    </div>
    <div class="control-row">
      <label>Size (USDC $)</label>
      <input type="number" id="mSize" min="5" max="500" step="5" value="10" style="width:100px">
    </div>
    <div class="btn-row">
      <button class="btn-save" onclick="submitManualTrade()">▶ Place Manual Trade</button>
    </div>
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

<!-- Transaction Log -->
<div class="section-pad">
  <div class="card">
    <h3>Transaction Log <span style="color:#8b949e;font-size:10px;font-weight:normal;margin-left:8px">All-time history</span>
      <button onclick="loadTxLog()" style="float:right;background:#1f6feb;color:#fff;padding:4px 10px;border:none;border-radius:4px;cursor:pointer;font-size:11px">Refresh</button>
    </h3>
    <div style="overflow-x:auto">
    <table>
      <thead><tr><th>Time</th><th>Coin</th><th>TF</th><th>Side</th><th>Mode</th><th>Entry</th><th>Size</th><th>PnL</th><th>Result</th></tr></thead>
      <tbody id="txLog"><tr><td colspan="9" style="color:#8b949e;text-align:center;padding:16px">Click Refresh to load</td></tr></tbody>
    </table>
    </div>
    <div id="txStats" style="margin-top:10px;font-size:12px;color:#8b949e;display:flex;gap:24px"></div>
  </div>
</div>

<!-- Win Rate Stats -->
<div class="section-pad">
  <div class="card">
    <h3>Win Rate by Coin / Timeframe / Mode
      <button onclick="loadStats()" style="float:right;background:#238636;color:#fff;padding:4px 14px;border:none;border-radius:4px;cursor:pointer;font-size:11px">Refresh</button>
    </h3>
    <div id="statsSummary" style="display:flex;gap:24px;margin-bottom:12px;flex-wrap:wrap;font-size:13px"></div>
    <table id="statsTable" style="width:100%;border-collapse:collapse;font-size:12px">
      <thead>
        <tr style="color:#8b949e;text-align:left;border-bottom:1px solid #30363d">
          <th style="padding:6px">Coin</th>
          <th style="padding:6px">TF</th>
          <th style="padding:6px">Mode</th>
          <th style="padding:6px">W</th>
          <th style="padding:6px">L</th>
          <th style="padding:6px">Win%</th>
          <th style="padding:6px">PnL</th>
        </tr>
      </thead>
      <tbody id="statsBody"><tr><td colspan="7" style="color:#8b949e;padding:8px">No trade data yet.</td></tr></tbody>
    </table>
  </div>
</div>

<!-- AI Trading Agent -->
<div class="section-pad">
  <div class="card">
    <h3>AI Trading Agent
      <button onclick="runAnalysis()" id="analyzeBtn" style="float:right;background:#6e40c9;color:#fff;padding:4px 14px;border:none;border-radius:4px;cursor:pointer;font-size:11px">Analyze Trades</button>
    </h3>
    <div id="aiStatus" style="color:#8b949e;font-size:12px;margin-bottom:8px">Click "Analyze Trades" to get AI insights on your trading performance.</div>
    <div id="aiStats" style="display:flex;gap:16px;margin-bottom:12px;flex-wrap:wrap"></div>
    <div id="aiAnalysis" style="background:#0d1117;border:1px solid #30363d;border-radius:4px;padding:12px;font-size:12px;line-height:1.6;white-space:pre-wrap;display:none;max-height:500px;overflow-y:auto"></div>
  </div>
</div>

<!-- Live Log -->
<div class="section-pad">
  <div class="card">
    <h3>Live Log</h3>
    <div style="display:flex;gap:6px;margin-bottom:10px;flex-wrap:wrap">
      <button onclick="setTab('all')"     id="tab-all"     class="log-tab active">All</button>
      <button onclick="setTab('prices')"  id="tab-prices"  class="log-tab">Prices (CLOB/Gamma)</button>
      <button onclick="setTab('orders')"  id="tab-orders"  class="log-tab">Orders</button>
      <button onclick="setTab('redeem')"  id="tab-redeem"  class="log-tab">Redeemed</button>
      <button onclick="setTab('errors')"  id="tab-errors"  class="log-tab">Errors</button>
    </div>
    <div id="log-all"    class="log-panel" style="display:block"></div>
    <div id="log-prices" class="log-panel" style="display:none"></div>
    <div id="log-orders" class="log-panel" style="display:none"></div>
    <div id="log-redeem" class="log-panel" style="display:none"></div>
    <div id="log-errors" class="log-panel" style="display:none"></div>
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

function _updateStateInner(s) {
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

  // Live wallet balance
  const cb = document.getElementById('cashBalance');
  if (s.cash_balance != null) {
    cb.textContent = `$${s.cash_balance.toFixed(2)} USDC`;
    cb.style.color = s.cash_balance > 10 ? '#3fb950' : '#e3b341';
  } else {
    cb.textContent = 'Unavailable';
    cb.style.color = '#8b949e';
  }

  // Config sliders
  if (s.config) {
    const me = document.getElementById('minEntry');
    me.value = s.config.min_entry;
    document.getElementById('minEntryVal').textContent = parseFloat(s.config.min_entry).toFixed(3);
    document.getElementById('totalCapital').value = s.config.total_capital;
    if (s.config.max_concurrent) document.getElementById('maxConcurrent').value = s.config.max_concurrent;
    if (s.config.snipe2 && !document.getElementById('s2MinPrice')._loaded) {
      const s2 = s.config.snipe2;
      document.getElementById('s2Enabled').checked  = s2.enabled !== false;
      document.getElementById('s2MinPrice').value   = s2.min_price  || 0.95;
      document.getElementById('s2PerTrade').value   = s2.max_per_trade  || 20;
      document.getElementById('s2PerMarket').value  = s2.max_per_market || 20;
      document.getElementById('s2Window').value     = s2.window_seconds || 10;
      document.getElementById('s2MinPrice')._loaded = true;
    }
    // Bot 1 (15m sniper) fields
    if (!document.getElementById('b1SniperMin')._loaded) {
      document.getElementById('b1SniperMin').value  = s.config.sniper_min || 0.97;
      document.getElementById('b1MinEntry').value   = s.config.min_entry  || 0.97;
      document.getElementById('b1PerTrade').value   = s.config.max_per_trade  || 20;
      document.getElementById('b1PerMarket').value  = s.config.max_per_market || 20;
      document.getElementById('b1SniperMin')._loaded = true;
    }
    // Bot 2 (1h) fields
    const s1h = s.config.snipe_1h || {};
    if (s1h.min_price != null && !document.getElementById('b2MinPrice')._loaded) {
      document.getElementById('b2Enabled').checked   = s1h.enabled !== false;
      document.getElementById('b2MinPrice').value    = s1h.min_price    || 0.89;
      document.getElementById('b2PerTrade').value    = s1h.max_per_trade  || 10;
      document.getElementById('b2PerMarket').value   = s1h.max_per_market || 50;
      document.getElementById('b2Window').value      = s1h.window_seconds || 1500;
      document.getElementById('b2MinPrice')._loaded  = true;
    }
    // Bot 2 current params display
    document.getElementById('b2Status').textContent       = s1h.enabled !== false ? '✅ Enabled' : '⛔ Disabled';
    document.getElementById('b2CurMinPrice').textContent  = s1h.min_price    != null ? s1h.min_price.toFixed(3)    : '—';
    document.getElementById('b2CurPerTrade').textContent  = s1h.max_per_trade  != null ? '$'+s1h.max_per_trade      : '—';
    document.getElementById('b2CurPerMarket').textContent = s1h.max_per_market != null ? '$'+s1h.max_per_market     : '—';
    document.getElementById('b2CurWindow').textContent    = s1h.window_seconds != null ? s1h.window_seconds+'s'    : '—';
    document.getElementById('b2Markets').textContent      = s.config.markets_1h_count != null ? s.config.markets_1h_count : '—';
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
        <td><button class="btn-exit" onclick="cancelOrder('${o.order_id}')">Cancel</button></td>
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

let _activeTab = 'all';

function setTab(tab) {
  _activeTab = tab;
  document.querySelectorAll('.log-panel').forEach(p => p.style.display = 'none');
  document.querySelectorAll('.log-tab').forEach(b => b.classList.remove('active'));
  document.getElementById('log-' + tab).style.display = 'block';
  document.getElementById('tab-' + tab).classList.add('active');
}

function appendLog(line) {
  const isPrices = line.includes('CLOB') || line.includes('Gamma') || line.includes('midpoint') ||
                   line.includes('Market refresh') || line.includes('price') || line.includes('DIAG');
  const isRedeem = line.includes('Redeem') || line.includes('redeem') || line.includes('redeemed');
  const isOrder  = line.includes('Order') || line.includes('SNIPER') || line.includes('filled') ||
                   line.includes('cancel') || line.includes('placed') || line.includes('submit');
  const isError  = line.includes('ERROR') || line.includes('exception') || line.includes('failed');

  const cls = isError ? 'error' : isRedeem ? 'redeem' : isOrder ? (line.includes('cancel') ? 'cancel' : 'trade') :
              isPrices ? 'price' : 'info';

  const panels = ['all'];
  if (isPrices) panels.push('prices');
  if (isOrder)  panels.push('orders');
  if (isRedeem) panels.push('redeem');
  if (isError)  panels.push('errors');

  const time = `[${new Date().toLocaleTimeString()}] `;
  panels.forEach(p => {
    const panel = document.getElementById('log-' + p);
    if (!panel) return;
    const div = document.createElement('div');
    div.className = `log-line ${cls}`;
    div.textContent = time + line;
    panel.appendChild(div);
    panel.scrollTop = panel.scrollHeight;
    if (panel.children.length > 300) panel.removeChild(panel.firstChild);
  });
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
function wakeBot() { api('/control/wake', {}); }

function switchBot(bot) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.getElementById(bot + 'Panel').classList.add('active');
  event.target.classList.add('active');
}

function saveBot1() {
  const msg = document.getElementById('bot1Msg');
  api('/settings/bot1', {
    sniper_min:  parseFloat(document.getElementById('b1SniperMin').value),
    min_entry:   parseFloat(document.getElementById('b1MinEntry').value),
    per_trade:   parseFloat(document.getElementById('b1PerTrade').value),
    per_market:  parseFloat(document.getElementById('b1PerMarket').value),
    window:      parseInt(document.getElementById('b1Window').value),
  }).then(d => { msg.textContent = d.message || 'Saved'; msg.style.color='#3fb950'; setTimeout(()=>msg.textContent='',4000); });
  // Reset loaded flag so next state push re-populates
  document.getElementById('b1SniperMin')._loaded = false;
}

function saveBot2() {
  const msg = document.getElementById('bot2Msg');
  api('/settings/snipe1h', {
    enabled:    document.getElementById('b2Enabled').checked,
    min_price:  parseFloat(document.getElementById('b2MinPrice').value),
    per_trade:  parseFloat(document.getElementById('b2PerTrade').value),
    per_market: parseFloat(document.getElementById('b2PerMarket').value),
    window:     parseInt(document.getElementById('b2Window').value),
  }).then(d => { msg.textContent = d.message || 'Saved'; msg.style.color='#3fb950'; setTimeout(()=>msg.textContent='',4000); });
  document.getElementById('b2MinPrice')._loaded = false;
}

function saveBudget() {
  const msg = document.getElementById('budgetMsg');
  api('/settings/budget', {
    total: parseFloat(document.getElementById('totalCapital').value),
    max_concurrent: parseInt(document.getElementById('maxConcurrent').value),
  }).then(d => { msg.textContent = d.message || 'Saved'; msg.style.color='#3fb950'; setTimeout(()=>msg.textContent='',4000); });
}

async function redeemAll() {
  document.getElementById('redeemResult').textContent = 'Redeeming all positions...';
  const resp = await fetch('/redeem/all', {method: 'POST', headers: {'Content-Type': 'application/json'}});
  const data = await resp.json();
  document.getElementById('redeemResult').textContent = data.message || data.detail || JSON.stringify(data);
}

async function manualRedeem() {
  const cid = document.getElementById('redeemConditionId').value.trim();
  if (!cid) { document.getElementById('redeemResult').textContent = 'Enter a condition ID first.'; return; }
  document.getElementById('redeemResult').textContent = 'Submitting...';
  const resp = await fetch('/redeem/manual', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({condition_id: cid})
  });
  const data = await resp.json();
  document.getElementById('redeemResult').textContent = data.message || data.detail || JSON.stringify(data);
}

function saveSnipe2() {
  api('/settings/snipe2', {
    enabled:    document.getElementById('s2Enabled').checked,
    min_price:  parseFloat(document.getElementById('s2MinPrice').value),
    per_trade:  parseFloat(document.getElementById('s2PerTrade').value),
    per_market: parseFloat(document.getElementById('s2PerMarket').value),
    window:     parseInt(document.getElementById('s2Window').value),
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

function cancelOrder(orderId) {
  if (!confirm(`Cancel order ${orderId}?`)) return;
  api('/orders/cancel', {order_id: orderId});
}

function updatePrices(prices) {
  if (!prices) return;
  const coins = ['BTC','ETH','SOL','XRP'];
  const tbody = document.getElementById('pricesTable');
  tbody.innerHTML = coins.map(coin => {
    const p = prices[coin] || {};
    const binance = p.binance != null ? '$' + p.binance.toLocaleString('en-US', {maximumFractionDigits:2}) : '—';
    const upAsk   = p.up_ask  != null ? p.up_ask.toFixed(3)  : '—';
    const downAsk = p.down_ask != null ? p.down_ask.toFixed(3) : '—';
    const mom = p.momentum_1m;
    const momStr = mom != null ? `<span style="color:${mom>=0?'#3fb950':'#f85149'}">${mom>=0?'+':''}${mom.toFixed(2)}%</span>` : '—';
    return `<tr>
      <td><b>${coin}</b></td>
      <td style="color:#58a6ff">${binance}</td>
      <td style="color:#3fb950">${upAsk}</td>
      <td style="color:#f85149">${downAsk}</td>
      <td>${momStr}</td>
    </tr>`;
  }).join('');
}

function submitManualTrade() {
  const coin = document.getElementById('mCoin').value;
  const tf   = document.getElementById('mTf').value;
  const dir  = document.getElementById('mDir').value;
  const price = parseFloat(document.getElementById('mPrice').value);
  const size  = parseFloat(document.getElementById('mSize').value);
  if (!confirm(`Place manual ${dir.toUpperCase()} order for ${coin} ${tf}\\nPrice: ${price}  Size: $${size}`)) return;
  api('/trade/manual', {coin, direction: dir, timeframe: tf, price, size});
}

function updateState(s) {
  state = s;
  updatePrices(s.prices);
  updateLogs(s.logs);
  _updateStateInner(s);
}

function updateLogs(logs) {
  if (!logs || !logs.length) return;
  logs.forEach(l => appendLog(l.msg));
}

// Poll state every 3s via HTTP as backup
setInterval(() => fetch('/state').then(r=>r.json()).then(d=>updateState(d)), 3000);

// Keepalive ping every 5 minutes — prevents browser tab sleep and proxy idle timeouts
setInterval(() => fetch('/ping').catch(()=>{}), 5 * 60 * 1000);

// Auto-load stats on page load, then refresh every 60s
loadStats();
setInterval(loadStats, 60000);

// Transaction Log
async function loadTxLog() {
  const tbody = document.getElementById('txLog');
  tbody.innerHTML = '<tr><td colspan="9" style="color:#8b949e;text-align:center;padding:12px">Loading...</td></tr>';
  const trades = await fetch('/trades/log').then(r=>r.json()).catch(()=>[]);
  if (!trades.length) {
    tbody.innerHTML = '<tr><td colspan="9" style="color:#8b949e;text-align:center;padding:16px">No trades recorded yet</td></tr>';
    return;
  }
  const rev = [...trades].reverse();
  tbody.innerHTML = rev.map(t => {
    const pnl = t.pnl != null ? t.pnl : null;
    const win = t.win;
    const ts = t.timestamp ? new Date(t.timestamp*1000).toLocaleString() : '—';
    return `<tr>
      <td style="font-size:11px;white-space:nowrap">${ts}</td>
      <td><b>${t.coin||'—'}</b></td>
      <td>${t.timeframe||'—'}</td>
      <td style="color:${(t.side||'yes')==='no'?'#f85149':'#3fb950'}">${(t.side||'YES').toUpperCase()}</td>
      <td><span class="tag tag-${t.mode||'standard'}">${t.mode||'—'}</span></td>
      <td>${t.entry_price!=null?t.entry_price.toFixed(4):'—'}</td>
      <td>$${t.entry_usdc!=null?t.entry_usdc.toFixed(2):'—'}</td>
      <td class="${pnl>=0?'win':'loss'}">${pnl!=null?(pnl>=0?'+':'')+pnl.toFixed(4):'—'}</td>
      <td class="${win?'win':'loss'}">${win?'✓ WIN':'✗ LOSS'}</td>
    </tr>`;
  }).join('');
  // Stats
  const wins = trades.filter(t=>t.win).length;
  const losses = trades.filter(t=>!t.win && t.pnl!=null).length;
  const totalPnl = trades.reduce((s,t)=>s+(t.pnl||0),0);
  const wr = trades.length ? Math.round(wins/trades.length*100) : 0;
  document.getElementById('txStats').innerHTML = `
    <span>Total: <b>${trades.length}</b></span>
    <span class="win">Wins: <b>${wins}</b></span>
    <span class="loss">Losses: <b>${losses}</b></span>
    <span>Win Rate: <b>${wr}%</b></span>
    <span class="${totalPnl>=0?'win':'loss'}">Total PnL: <b>${totalPnl>=0?'+':''}$${totalPnl.toFixed(4)}</b></span>
  `;
}

// AI Analysis
async function loadStats() {
  try {
    const d = await fetch('/stats').then(r => r.json());
    const s = d.summary || {};
    const pnlColor = (s.total_pnl || 0) >= 0 ? '#3fb950' : '#f85149';
    document.getElementById('statsSummary').innerHTML = `
      <span style="background:#161b22;border:1px solid #30363d;border-radius:4px;padding:4px 12px">Trades: <b>${s.total_trades||0}</b></span>
      <span style="background:#161b22;border:1px solid #30363d;border-radius:4px;padding:4px 12px;color:#3fb950">Wins: <b>${s.wins||0}</b></span>
      <span style="background:#161b22;border:1px solid #30363d;border-radius:4px;padding:4px 12px;color:#f85149">Losses: <b>${s.losses||0}</b></span>
      <span style="background:#161b22;border:1px solid #30363d;border-radius:4px;padding:4px 12px">Win Rate: <b>${s.win_rate_pct||0}%</b></span>
      <span style="background:#161b22;border:1px solid #30363d;border-radius:4px;padding:4px 12px;color:${pnlColor}">PnL: <b>${(s.total_pnl||0)>=0?'+':''}$${s.total_pnl||0}</b></span>
    `;
    const rows = d.rows || [];
    if (!rows.length) return;
    document.getElementById('statsBody').innerHTML = rows.map(r => {
      const wr = r.win_rate_pct;
      const wrColor = wr >= 70 ? '#3fb950' : wr >= 50 ? '#e3b341' : '#f85149';
      const pColor = r.pnl >= 0 ? '#3fb950' : '#f85149';
      return `<tr style="border-bottom:1px solid #21262d">
        <td style="padding:6px"><b>${r.coin}</b></td>
        <td style="padding:6px;color:#8b949e">${r.timeframe}</td>
        <td style="padding:6px;color:#8b949e">${r.mode}</td>
        <td style="padding:6px;color:#3fb950">${r.wins}</td>
        <td style="padding:6px;color:#f85149">${r.losses}</td>
        <td style="padding:6px;color:${wrColor};font-weight:bold">${wr}%</td>
        <td style="padding:6px;color:${pColor}">${r.pnl>=0?'+':''}$${r.pnl}</td>
      </tr>`;
    }).join('');
  } catch(e) { console.error('Stats load error', e); }
}

async function runAnalysis() {
  const btn = document.getElementById('analyzeBtn');
  const status = document.getElementById('aiStatus');
  const box = document.getElementById('aiAnalysis');
  btn.disabled = true; btn.textContent = 'Analyzing...';
  status.textContent = 'Running AI analysis... this may take 10-20 seconds';
  box.style.display = 'none';
  try {
    const res = await fetch('/analyze', {method:'POST'}).then(r=>r.json());
    if (res.analysis) {
      box.textContent = res.analysis;
      box.style.display = 'block';
      status.textContent = 'Analysis complete';
      if (res.stats) {
        document.getElementById('aiStats').innerHTML = `
          <span style="background:#161b22;border:1px solid #30363d;border-radius:4px;padding:4px 10px">Trades: <b>${res.stats.total_trades}</b></span>
          <span style="background:#161b22;border:1px solid #30363d;border-radius:4px;padding:4px 10px" class="win">Wins: <b>${res.stats.wins}</b></span>
          <span style="background:#161b22;border:1px solid #30363d;border-radius:4px;padding:4px 10px" class="loss">Losses: <b>${res.stats.losses}</b></span>
          <span style="background:#161b22;border:1px solid #30363d;border-radius:4px;padding:4px 10px">Win Rate: <b>${res.stats.win_rate_pct}%</b></span>
          <span style="background:#161b22;border:1px solid #30363d;border-radius:4px;padding:4px 10px" class="${res.stats.total_pnl_usdc>=0?'win':'loss'}">PnL: <b>${res.stats.total_pnl_usdc>=0?'+':''}$${res.stats.total_pnl_usdc}</b></span>
        `;
      }
    }
  } catch(e) {
    status.textContent = 'Error: ' + e.message;
  }
  btn.disabled = false; btn.textContent = 'Analyze Trades';
}
</script>
</body>
</html>
"""


# ------------------------------------------------------------------
# API ROUTES
@app.get("/debug/markets")
async def debug_markets():
    """Fetch markets using time-filtered queries to find short-term crypto markets."""
    import httpx as _httpx
    import time as _time
    now = int(_time.time())

    async with _httpx.AsyncClient(timeout=10.0) as client:
        # Short window query
        r1 = await client.get(
            "https://gamma-api.polymarket.com/markets",
            params={"active": True, "closed": False, "limit": 100,
                    "end_date_min": now, "end_date_max": now + 1800,
                    "order": "end_date_asc"}
        )
        d1 = r1.json()
        short_markets = d1 if isinstance(d1, list) else d1.get("markets", [])

        # 2 hour window
        r2 = await client.get(
            "https://gamma-api.polymarket.com/markets",
            params={"active": True, "closed": False, "limit": 100,
                    "end_date_min": now, "end_date_max": now + 7200,
                    "order": "end_date_asc"}
        )
        d2 = r2.json()
        medium_markets = d2 if isinstance(d2, list) else d2.get("markets", [])

    return {
        "next_30min_count": len(short_markets),
        "next_2hr_count": len(medium_markets),
        "next_30min_questions": [
            {"q": m.get("question"), "end": m.get("endDate")}
            for m in short_markets[:20]
        ],
        "next_2hr_questions": [
            {"q": m.get("question"), "end": m.get("endDate")}
            for m in medium_markets[:20]
        ],
    }


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
    clob_ready = False
    markets_tracked = 0
    last_scan_age = None
    halted = False
    paused = False
    binance_ready = []
    binance_stale = []

    if bot:
        wallet = getattr(bot.execution, "_wallet_address", "")
        clob_ready = getattr(bot.execution, "_clob", None) is not None
        markets_tracked = len(bot.poly_listener.markets)
        last_scan_age = round(time.time() - bot._last_scan_time, 1)
        halted = bot.risk_manager.is_halted
        paused = bot.risk_manager.is_paused
        coins = bot.config.get("markets", {}).get("coins", [])
        binance_ready = [c for c in coins if bot.binance.is_ready(c)]
        binance_stale = [c for c in coins if not bot.binance.is_ready(c)]

    return {
        "bot_running":              bot is not None,
        "clob_client_ready":        clob_ready,
        "wallet_address":           wallet or "not loaded",
        "markets_tracked":          markets_tracked,
        "last_scan_seconds_ago":    last_scan_age,
        "halted":                   halted,
        "paused":                   paused,
        "binance_ready":            binance_ready,
        "binance_stale":            binance_stale,
        "POLYMARKET_PRIVATE_KEY":   masked(pk),
        "POLYMARKET_API_KEY":       masked(ak),
        "POLYMARKET_API_SECRET":    masked(sec),
        "POLYMARKET_API_PASSPHRASE": masked(pw),
        "TELEGRAM_BOT_TOKEN":       masked(tg),
        "POLYGON_RPC_URL":          rpc if rpc else "MISSING (using default)",
        "trading_enabled":          bool(pk and ak and sec and pw),
        "telegram_enabled":         bool(tg),
    }


@app.get("/state")
async def get_state():
    bot = get_bot()
    if not bot:
        return {"error": "Bot not running"}
    state = bot.get_state()
    state["cash_balance"] = await bot.execution.get_usdc_balance()
    state["logs"] = list(_log_buffer)[-100:]  # last 100 lines
    return state


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


@app.post("/control/wake")
async def wake_bot():
    """Reset all stopped states and force the bot back to active trading."""
    bot = get_bot()
    if not bot:
        raise HTTPException(503, "Bot not running")
    bot.risk_manager.reset_halt()
    bot.risk_manager._paused = False
    return {"message": "Bot woken — all halts and pauses cleared"}


@app.post("/settings/price")
async def update_price(data: PriceRangeUpdate):
    if data.min_entry < 0.90:
        raise HTTPException(400, "min_entry cannot be below 0.90")
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
    bot = get_bot()
    if bot:
        bot.config["capital"]["total"] = data.total
        bot.config["capital"]["max_concurrent_trades"] = data.max_concurrent
    return {"message": f"Capital updated — total=${data.total}, max_concurrent={data.max_concurrent}"}


@app.post("/settings/bot1")
async def update_bot1(data: Bot1Update):
    if data.sniper_min < 0.80 or data.min_entry < 0.80:
        raise HTTPException(400, "Price floors cannot be below 0.80")
    config = load_config()
    config["price"]["sniper_min"] = round(data.sniper_min, 4)
    config["price"]["min_entry"] = round(data.min_entry, 4)
    config["capital"]["max_per_trade"] = data.per_trade
    config["capital"]["max_per_market"] = data.per_market
    config["sniper"]["order_timeout_seconds"] = data.window
    save_config(config)
    bot = get_bot()
    if bot:
        bot.config["price"]["sniper_min"] = round(data.sniper_min, 4)
        bot.config["price"]["min_entry"] = round(data.min_entry, 4)
        bot.config["capital"]["max_per_trade"] = data.per_trade
        bot.config["capital"]["max_per_market"] = data.per_market
    return {"message": f"Bot 1 updated — sniper_min={data.sniper_min}, min_entry={data.min_entry}, ${data.per_trade}/trade, window={data.window}s"}


@app.post("/settings/snipe1h")
async def update_snipe1h(data: Snipe1hUpdate):
    if data.min_price < 0.50:
        raise HTTPException(400, "min_price cannot be below 0.50")
    config = load_config()
    config["snipe_1h"] = {
        "enabled":        data.enabled,
        "min_price":      round(data.min_price, 3),
        "max_per_trade":  data.per_trade,
        "max_per_market": data.per_market,
        "window_seconds": data.window,
    }
    save_config(config)
    bot = get_bot()
    if bot:
        bot.config["snipe_1h"] = config["snipe_1h"]
    status = "enabled" if data.enabled else "disabled"
    return {"message": f"Bot 2 updated — {status}, min={data.min_price}, ${data.per_trade}/trade, window={data.window}s"}


@app.post("/settings/trade")
async def update_trade(data: TradeUpdate):
    config = load_config()
    config["risk"]["kelly"]["score_95_100"] = round(data.kelly_score_95, 4)
    config["risk"]["kelly"]["score_90_94"] = round(data.kelly_score_90, 4)
    config["risk"]["kelly"]["score_85_89"] = round(data.kelly_score_85, 4)
    save_config(config)
    return {"message": "Kelly sizing updated"}


@app.post("/trade/manual")
async def manual_trade(data: ManualTradeRequest):
    bot = get_bot()
    if not bot:
        raise HTTPException(503, "Bot not running")
    if data.price < 0.50 or data.price > 0.99:
        raise HTTPException(400, "Price must be between 0.50 and 0.99")
    if data.size < 5:
        raise HTTPException(400, "Minimum size is $5")
    if data.coin not in ["BTC", "ETH", "SOL", "XRP"]:
        raise HTTPException(400, f"Unknown coin: {data.coin}")
    if data.direction not in ["up", "down"]:
        raise HTTPException(400, "Direction must be 'up' or 'down'")

    # Find matching market
    market = next(
        (m for m in bot.poly_listener.markets.values()
         if m.coin == data.coin and m.timeframe == data.timeframe
         and not m.is_expired),
        None
    )
    if not market:
        raise HTTPException(404, f"No active {data.coin} {data.timeframe} market found. "
                                 f"Available: {[f'{m.coin}{m.timeframe}' for m in bot.poly_listener.markets.values() if not m.is_expired]}")

    # For DOWN direction use the no_token (Down token); UP uses yes_token (Up token)
    token_id = market.no_token_id if data.direction == "down" else market.yes_token_id
    if not token_id:
        raise HTTPException(500, f"No token ID for {data.direction} direction on {data.coin}")

    order = await bot.execution.place_limit_order(
        token_id=token_id,
        market_id=market.market_id,
        price=data.price,
        size=data.size,
        mode="manual",
    )
    if order:
        return {"message": f"Manual {data.direction.upper()} order placed for {data.coin} {data.timeframe} @ {data.price} size=${data.size} | order_id={order.order_id}"}

    raise HTTPException(500, f"Order placement failed — check Railway logs for details (API key, balance, or signing error)")


TRADE_LOG_PATH = Path("logs/trades.jsonl")


def read_trade_log(limit: int = 200) -> list:
    if not TRADE_LOG_PATH.exists():
        return []
    trades = []
    try:
        with open(TRADE_LOG_PATH) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        trades.append(json.loads(line))
                    except Exception:
                        pass
    except Exception:
        pass
    return trades[-limit:]


@app.get("/trades/log")
async def get_trade_log():
    return read_trade_log(200)


@app.post("/analyze")
async def analyze_trades():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise HTTPException(503, "ANTHROPIC_API_KEY not set in Railway env vars")
    trades = read_trade_log(100)
    if not trades:
        return {"analysis": "No trade history yet. The AI agent will analyze your trades once you have some completed."}
    try:
        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=api_key)

        wins   = [t for t in trades if t.get("win")]
        losses = [t for t in trades if not t.get("win") and t.get("pnl") is not None]
        total_pnl = sum(t.get("pnl", 0) or 0 for t in trades)
        win_rate  = round(len(wins) / len(trades) * 100, 1) if trades else 0

        summary = {
            "total_trades": len(trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate_pct": win_rate,
            "total_pnl_usdc": round(total_pnl, 4),
            "recent_trades": trades[-20:],
        }

        prompt = f"""You are an expert Polymarket trading analyst reviewing a sniper bot's performance.

The bot trades Up/Down crypto markets (BTC, ETH, SOL, XRP) on 5-minute and 15-minute timeframes.
It buys whichever token (UP or DOWN) is priced at 0.98+ — meaning the market has nearly decided.
A winning trade resolves at 1.0 (profit = ~2%), a losing trade resolves at 0.0 (total loss).

Performance summary:
{json.dumps(summary, indent=2)}

Please provide:
1. **Key Learning Points** — what patterns do you see in wins vs losses?
2. **Risk Assessment** — is the strategy sustainable? What are the main risks?
3. **Specific Suggestions** — concrete parameter changes or strategy improvements
4. **Market Timing** — which coins/timeframes perform best?

Be concise and actionable. Format with clear headers."""

        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )
        return {"analysis": msg.content[0].text, "stats": summary}
    except Exception as e:
        raise HTTPException(500, f"Analysis failed: {e}")


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


class CancelOrderRequest(BaseModel):
    order_id: str


@app.post("/orders/cancel")
async def cancel_order(data: CancelOrderRequest):
    bot = get_bot()
    if not bot:
        raise HTTPException(503, "Bot not running")
    pos = bot.order_manager.active_orders.get(data.order_id)
    if not pos:
        raise HTTPException(404, f"Active order {data.order_id} not found")
    success = await bot.execution.cancel_order(pos.order)
    if success:
        bot.order_manager.active_orders.pop(data.order_id, None)
        return {"message": f"Order {data.order_id} cancelled"}
    raise HTTPException(500, "Cancel failed")


class Snipe2Update(BaseModel):
    enabled: bool = True
    min_price: float = 0.95
    per_trade: float = 10.0
    per_market: float = 20.0
    window: int = 10


@app.post("/settings/snipe2")
async def update_snipe2(data: Snipe2Update):
    config = load_config()
    config["snipe2"] = {
        "enabled":    data.enabled,
        "min_price":  round(data.min_price, 3),
        "max_per_trade":  data.per_trade,
        "max_per_market": data.per_market,
        "window_seconds": data.window,
    }
    save_config(config)
    bot = get_bot()
    if bot:
        bot.config["snipe2"] = config["snipe2"]
    return {"message": f"Snipe 2 updated — {'enabled' if data.enabled else 'disabled'}, min={data.min_price}, ${data.per_trade}/trade"}


class ManualRedeemRequest(BaseModel):
    condition_id: str


@app.api_route("/ping", methods=["GET", "POST", "HEAD"])
async def ping():
    return {"status": "ok"}


@app.get("/stats")
async def get_stats():
    """Win/loss stats per coin, timeframe and mode."""
    bot = get_bot()
    if not bot:
        return {"summary": {}, "rows": []}
    tracker = bot.structured_log.win_rate
    return {"summary": tracker.summary(), "rows": tracker.get_stats()}


@app.post("/redeem/all")
async def redeem_all():
    bot = get_bot()
    if not bot:
        raise HTTPException(503, "Bot not running")

    proxy_wallet = os.environ.get("POLYMARKET_PROXY_WALLET", "")
    if not proxy_wallet:
        raise HTTPException(400, "POLYMARKET_PROXY_WALLET not set")

    # Fetch ALL redeemable positions from Polymarket data API
    import httpx as _httpx
    try:
        async with _httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                "https://data-api.polymarket.com/positions",
                params={"user": proxy_wallet, "redeemable": "true", "limit": 500},
            )
        positions = resp.json() if resp.status_code == 200 else []
    except Exception as e:
        raise HTTPException(500, f"Failed to fetch positions: {e}")

    if not positions:
        return {"message": "No redeemable positions found on Polymarket"}

    attempted = 0
    failed = 0
    for p in positions:
        cid = p.get("conditionId") or p.get("condition_id", "")
        if not cid:
            continue
        success = await bot.execution.redeem_position(cid, [])
        if success:
            attempted += 1
        else:
            failed += 1
        await asyncio.sleep(2)  # avoid rate limiting

    return {"message": f"Redeemed {attempted} position(s). Failed: {failed}. Check logs for tx hashes."}


@app.post("/redeem/manual")
async def manual_redeem(data: ManualRedeemRequest):
    bot = get_bot()
    if not bot:
        raise HTTPException(503, "Bot not running")
    if not data.condition_id or len(data.condition_id) < 10:
        raise HTTPException(400, "Invalid condition_id")
    success = await bot.execution.redeem_position(data.condition_id, [])
    if success:
        return {"message": f"Redeem tx sent for condition {data.condition_id[:16]}..."}
    raise HTTPException(500, "Redeem failed — check logs for details")


# ------------------------------------------------------------------
# WEBSOCKET for live log + state push
# ------------------------------------------------------------------
active_ws: list[WebSocket] = []


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_ws.append(websocket)
    try:
        bot = get_bot()
        if bot:
            s = bot.get_state()
            s["cash_balance"] = await bot.execution.get_usdc_balance()
            await websocket.send_text(json.dumps({
                "type": "state", "payload": s
            }))
        while True:
            await asyncio.sleep(2)
            bot = get_bot()
            if bot:
                s = bot.get_state()
                s["cash_balance"] = await bot.execution.get_usdc_balance()
                s["logs"] = list(_log_buffer)[-100:]
                await websocket.send_text(json.dumps({
                    "type": "state", "payload": s
                }))
    except WebSocketDisconnect:
        if websocket in active_ws:
            active_ws.remove(websocket)
    except Exception:
        if websocket in active_ws:
            active_ws.remove(websocket)


async def broadcast_log(message: str):
    for ws in list(active_ws):
        try:
            await ws.send_text(json.dumps({"type": "log", "payload": message}))
        except Exception:
            if ws in active_ws:
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
