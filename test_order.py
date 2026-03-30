"""
test_order.py
Quick smoke-test: place a single $10 limit order on the current BTC 5m market.
Run: python test_order.py
"""
import asyncio
import os
import sys
import time
import httpx
from dotenv import load_dotenv

load_dotenv()

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"


async def main():
    print("=" * 55)
    print("Polymarket Bot — Order Test")
    print("=" * 55)

    # 1. Check env vars
    required = ["POLYMARKET_PRIVATE_KEY", "POLYMARKET_API_KEY",
                "POLYMARKET_API_SECRET", "POLYMARKET_API_PASSPHRASE"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        print(f"[FAIL] Missing env vars: {missing}")
        sys.exit(1)
    print(f"[OK]   All {len(required)} env vars present")

    proxy = os.environ.get("PROXY_URL", "")
    client = httpx.AsyncClient(
        timeout=10.0,
        proxy=proxy if proxy else None
    )
    if proxy:
        print(f"[OK]   Using proxy: {proxy[:40]}...")

    # 2. Find current BTC 5m market
    epoch = (int(time.time()) // 300) * 300
    slug = f"btc-updown-5m-{epoch}"
    print(f"\n[..] Fetching market: {slug}")
    try:
        resp = await client.get(f"{GAMMA_BASE}/events/slug/{slug}")
        if resp.status_code == 404:
            epoch += 300
            slug = f"btc-updown-5m-{epoch}"
            print(f"[..] 404, trying next window: {slug}")
            resp = await client.get(f"{GAMMA_BASE}/events/slug/{slug}")
        resp.raise_for_status()
        event = resp.json()
        markets = event.get("markets", [])
        if not markets:
            print("[FAIL] No markets in event")
            sys.exit(1)
        m = markets[0]
        import json
        token_ids = json.loads(m.get("clobTokenIds", "[]"))
        yes_token = str(token_ids[0])
        market_id = str(m.get("id", ""))
        best_ask  = float(m.get("bestAsk") or m.get("lastTradePrice") or 0.60)
        print(f"[OK]   Market: {event.get('title')}")
        print(f"[OK]   Market ID: {market_id}")
        print(f"[OK]   UP token: {yes_token[:20]}...")
        print(f"[OK]   Best ask: {best_ask}")
        if m.get("closed"):
            print("[WARN] Market is closed — trying next window")
    except Exception as e:
        print(f"[FAIL] Market fetch error: {e}")
        sys.exit(1)

    # 3. Test CLOB connectivity
    print(f"\n[..] Testing CLOB connectivity...")
    try:
        r = await client.get(f"{CLOB_BASE}/markets?limit=1")
        print(f"[OK]   CLOB reachable: HTTP {r.status_code}")
    except Exception as e:
        print(f"[FAIL] CLOB unreachable: {e}")
        sys.exit(1)

    # 4. Place $10 test order
    print(f"\n[..] Placing $10 limit order (UP @ {best_ask:.3f})...")
    try:
        import yaml, sys
        sys.path.insert(0, ".")
        with open("config.yaml") as f:
            config = yaml.safe_load(f)
        from execution_engine import ExecutionEngine
        engine = ExecutionEngine(config)

        order = await engine.place_limit_order(
            token_id=yes_token,
            market_id=market_id,
            price=best_ask,
            size=10.0,
            mode="manual",
        )
        if order:
            print(f"\n[OK]   ORDER PLACED SUCCESSFULLY")
            print(f"       Order ID : {order.order_id}")
            print(f"       Price    : {order.price}")
            print(f"       Shares   : {order.size:.4f}")
            print(f"\n[..] Cancelling test order...")
            cancelled = await engine.cancel_order(order)
            print(f"[OK]   Cancelled: {cancelled}")
        else:
            print("[FAIL] place_limit_order returned None — check logs above")
    except Exception as e:
        print(f"[FAIL] Exception during order: {e}")
    finally:
        await client.aclose()

    print("\n" + "=" * 55)


asyncio.run(main())
