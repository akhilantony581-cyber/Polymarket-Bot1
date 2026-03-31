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
    """L1 auth: sign timestamp+method+path+body with private key (EIP-191)."""
    timestamp = str(int(time.time()))
    message = timestamp + method.upper() + path + body
    msg = encode_defunct(text=message)
    signed = account.sign_message(msg)
    return {
        "POLY-ADDRESS": account.address,
        "POLY-SIGNATURE": signed.signature.hex(),
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
        path = "/auth/api-key"

        # POST creates (or re-derives) API credentials
        print(f"\n[..] Deriving API key from {CLOB_BASE}{path} ...")
        headers = _l1_headers(account, "POST", path)
        headers["Content-Type"] = "application/json"
        resp = await client.post(f"{CLOB_BASE}{path}", headers=headers, json={"nonce": 0})
        print(f"     Status: {resp.status_code}  Body: {resp.text[:300]}")

        if resp.status_code not in (200, 201):
            print(f"[FAIL] Could not derive API key: {resp.text}")
            sys.exit(1)
        data = resp.json()

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
