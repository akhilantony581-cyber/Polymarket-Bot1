"""
approve_usdc.py
Checks USDC balance and approves the Polymarket CTF Exchange
to spend USDC from your wallet on Polygon.

Run: python3 approve_usdc.py
"""
import os
import sys
from dotenv import load_dotenv

load_dotenv()

try:
    from web3 import Web3
except ImportError:
    print("[FAIL] Run: pip3 install web3")
    sys.exit(1)

POLYGON_RPC = "https://polygon-rpc.com"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC on Polygon
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"  # Polymarket exchange

USDC_ABI = [
    {"name": "balanceOf", "type": "function", "inputs": [{"name": "account", "type": "address"}], "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view"},
    {"name": "allowance",  "type": "function", "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}], "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view"},
    {"name": "approve",    "type": "function", "inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}], "outputs": [{"name": "", "type": "bool"}], "stateMutability": "nonpayable"},
]

private_key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
if not private_key:
    print("[FAIL] POLYMARKET_PRIVATE_KEY not set in .env")
    sys.exit(1)

w3 = Web3(Web3.HTTPProvider(POLYGON_RPC))
account = w3.eth.account.from_key(private_key)
wallet = account.address
print(f"Wallet : {wallet}")

usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_ADDRESS), abi=USDC_ABI)

balance   = usdc.functions.balanceOf(wallet).call()
allowance = usdc.functions.allowance(wallet, Web3.to_checksum_address(CTF_EXCHANGE)).call()

print(f"USDC Balance  : ${balance / 1e6:.2f}")
print(f"USDC Allowance: ${allowance / 1e6:.2f} (for CTF Exchange)")

if balance == 0:
    print("\n[FAIL] No USDC in this wallet on Polygon. Withdraw from Polymarket first.")
    sys.exit(1)

if allowance > 0:
    print("\n[OK] Allowance already set — the bot should be able to place orders.")
    sys.exit(0)

print("\n[..] Allowance is 0 — approving max USDC for CTF Exchange...")

MAX_UINT256 = 2**256 - 1
nonce = w3.eth.get_transaction_count(wallet)
tx = usdc.functions.approve(
    Web3.to_checksum_address(CTF_EXCHANGE), MAX_UINT256
).build_transaction({
    "from": wallet,
    "nonce": nonce,
    "gas": 100000,
    "gasPrice": w3.to_wei("50", "gwei"),
    "chainId": 137,
})
signed = account.sign_transaction(tx)
tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
print(f"[OK] Approval tx sent: {tx_hash.hex()}")
print(f"     View: https://polygonscan.com/tx/{tx_hash.hex()}")
receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
print(f"[OK] Confirmed in block {receipt['blockNumber']}")
print("\nAllowance set! Redeploy the bot and orders should go through.")
