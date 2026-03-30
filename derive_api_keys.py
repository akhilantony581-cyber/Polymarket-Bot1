"""
derive_api_keys.py
Derives fresh Polymarket CLOB API credentials from your private key.

Run once locally:
    python derive_api_keys.py

Then copy the output values into Railway environment variables:
    POLYMARKET_API_KEY
    POLYMARKET_API_SECRET
    POLYMARKET_API_PASSPHRASE
"""
import asyncio
import hashlib
import json
import os
import sys
import time

import httpx
from dotenv import load_dotenv
from eth_account import Account
from eth_account.messages import encode_defunct

load_dotenv()

CLOB_BASE = "https://clob.polymarket.com"


def _l1_headers(account, method: str, path: str, body: str = "") -> dict:
    """L1 auth: signs timestamp+method+path+body with private key (EOA)."""
    timestamp = str(int(time.time()))
    message = timestamp + method.upper() + path + body
    msg = encode_defunct(text=message)
    signed = account.sign_message(msg)
    signature = signed.signature.hex()
    return {
        "POLY-ADDRESS": account.address,
        "POLY-SIGNATURE": signature,
        "POLY-TIMESTAMP": timestamp,
    }


async def main():
    private_key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
    if not private_key:
        print("[FAIL] POLYMARKET_PRIVATE_KEY not set")
        sys.exit(1)

    account = Account.from_key(private_key)
    print(f"Wallet: {account.address}")

    proxy = os.environ.get("PROXY_URL", "")
    client = httpx.AsyncClient(
        timeout=15.0,
        proxy=proxy if proxy else None,
    )

    try:
        # Try GET first — returns existing key if one exists
        path = "/auth/api-key"
        headers = _l1_headers(account, "GET", path)
        headers["Content-Type"] = "application/json"

        print(f"\n[..] Fetching existing API key from {CLOB_BASE}{path} ...")
        resp = await client.get(f"{CLOB_BASE}{path}", headers=headers)
        print(f"     Status: {resp.status_code}")

        if resp.status_code == 200:
            data = resp.json()
        elif resp.status_code in (401, 404):
            # No key yet — create one
            print("[..] No existing key found. Creating new API key...")
            headers = _l1_headers(account, "GET", path)
            headers["Content-Type"] = "application/json"
            resp2 = await client.post(f"{CLOB_BASE}{path}", headers=headers)
            print(f"     Status: {resp2.status_code}  Body: {resp2.text[:300]}")
            if resp2.status_code not in (200, 201):
                print(f"[FAIL] Could not create API key: {resp2.text}")
                sys.exit(1)
            data = resp2.json()
        else:
            print(f"[FAIL] Unexpected response: {resp.status_code}  {resp.text[:300]}")
            sys.exit(1)

        api_key        = data.get("apiKey") or data.get("api_key", "")
        api_secret     = data.get("secret") or data.get("api_secret", "")
        api_passphrase = data.get("passphrase") or data.get("api_passphrase", "")

        if not api_key:
            print(f"[FAIL] Unexpected response shape: {data}")
            sys.exit(1)

        print("\n" + "=" * 60)
        print("SUCCESS — copy these into Railway environment variables:")
        print("=" * 60)
        print(f"POLYMARKET_API_KEY        = {api_key}")
        print(f"POLYMARKET_API_SECRET     = {api_secret}")
        print(f"POLYMARKET_API_PASSPHRASE = {api_passphrase}")
        print("=" * 60)

    finally:
        await client.aclose()


asyncio.run(main())
