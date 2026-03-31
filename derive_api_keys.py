"""
derive_api_keys.py
Derives fresh Polymarket CLOB API credentials from your private key.

Run once locally:
    pip3 install py-clob-client
    python3 derive_api_keys.py

Then copy the output values into Railway environment variables.
"""
import os
import sys
from dotenv import load_dotenv

load_dotenv()

private_key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
if not private_key:
    print("[FAIL] POLYMARKET_PRIVATE_KEY not set in .env")
    sys.exit(1)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
except ImportError:
    print("[FAIL] Run: pip3 install py-clob-client")
    sys.exit(1)

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet

client = ClobClient(CLOB_HOST, key=private_key, chain_id=CHAIN_ID)

try:
    creds = client.create_or_derive_api_creds()
except Exception:
    # Older versions of py-clob-client use different method name
    try:
        creds = client.derive_api_key()
    except Exception as e:
        print(f"[FAIL] Could not derive API key: {e}")
        sys.exit(1)

print("\n" + "=" * 60)
print("SUCCESS — copy these into Railway environment variables:")
print("=" * 60)
print(f"POLYMARKET_API_KEY        = {creds.api_key}")
print(f"POLYMARKET_API_SECRET     = {creds.api_secret}")
print(f"POLYMARKET_API_PASSPHRASE = {creds.api_passphrase}")
print("=" * 60)
