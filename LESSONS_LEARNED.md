# Polymarket Bot — Lessons Learned

A reference document covering every major error encountered building this bot,
root causes, and the exact fixes applied. Use this as a starting point for any
future Polymarket bot to avoid repeating the same debugging cycles.

---

## 1. Market Discovery — 0 Markets Tracked

**Time wasted:** Several hours across multiple iterations.

**Symptom:**
```
PolymarketListener: 0 markets found
```

**Root cause:**
The standard Gamma API endpoint `GET /markets?active=true` returns ~1500
long-term political/sports markets. Crypto 5m/15m markets never appear
in this list because they are short-lived recurring series.

**Failed attempts:**
- Filtering by keyword (`bitcoin`, `btc`, `5-minute`) — no results
- Pagination through all 1500 markets — crypto not present
- Using `end_date_max` filter — still empty

**Working solution:**
Construct slugs directly. Polymarket recurring crypto markets follow a
deterministic pattern:
```
{coin}-updown-{timeframe}-{epoch}
```
Where `epoch` is the Unix timestamp of the window start, aligned to the
timeframe boundary:
- 5m:  `(int(time.time()) // 300) * 300`
- 15m: `(int(time.time()) // 900) * 900`

Fetch current + next window for each coin/timeframe:
```python
slug = f"btc-updown-5m-1774900800"
resp = await client.get(f"https://gamma-api.polymarket.com/events/slug/{slug}")
```

Fetch 16 slugs total: 4 coins × 2 timeframes × 2 windows (current + next).

---

## 2. `clobTokenIds` is a JSON String, Not a List

**Symptom:** `token_ids[0]` throws `TypeError: string indices must be integers`

**Root cause:**
The Gamma API returns `clobTokenIds` as a **JSON-encoded string**, not a list:
```json
"clobTokenIds": "[\"123456...\", \"789012...\"]"
```

**Fix:**
```python
raw = m.get("clobTokenIds", "[]")
if isinstance(raw, str):
    token_ids = json.loads(raw)
else:
    token_ids = raw
yes_token = str(token_ids[0])  # UP token
no_token  = str(token_ids[1])  # DOWN token
```

---

## 3. Markets Accumulating (16 → 32 → 48…)

**Symptom:** Market count keeps growing every refresh cycle.

**Root cause:**
Expired markets were never removed from the `self.markets` dict.
Each cycle added new markets without pruning old ones.

**Fix:** After each refresh cycle, prune expired markets:
```python
expired_ids = [mid for mid, m in self.markets.items() if m.is_expired]
for mid in expired_ids:
    del self.markets[mid]
```

---

## 4. EIP-712 Signing — Multiple Failed Attempts

**Time wasted:** 3 separate attempts over several hours.

**Symptom:** `AttributeError: 'LocalAccount' object has no attribute 'sign_typed_data'`
then: `ImportError: cannot import name 'encode_typed_data'`

**Root cause:**
Different versions of `eth-account` expose different signing APIs.
`sign_typed_data` and `encode_typed_data` are not stable across versions.

**Working solution** (stable across all eth-account versions):
```python
from eth_account.messages import encode_structured_data

structured = {
    "types": {
        "EIP712Domain": [...],
        "Order": order_types["Order"],
    },
    "domain": domain,
    "primaryType": "Order",
    "message": order_struct,
}
msg = encode_structured_data(structured)
signed = account.sign_message(msg)
signature = signed.signature.hex()
```

**Note:** We eventually switched to `py-clob-client==0.28.0` which handles
all signing internally — recommended approach for future bots.

---

## 5. L2 HMAC Authentication — 401 Unauthorized

**Time wasted:** Several hours of debugging.

**Symptom:** `401 Unauthorized — Invalid api key` on every order placement.

**Root cause (two bugs):**
1. `self._api_secret` must be **base64-decoded** before use as HMAC key
2. The resulting digest must be **base64-encoded** (not hex) as the signature

**Broken code:**
```python
signature = hmac.new(
    self._api_secret.encode("utf-8"),   # WRONG — should be b64decode
    message.encode("utf-8"),
    digestmod=hashlib.sha256,
).hexdigest()                           # WRONG — should be b64encode
```

**Fixed code:**
```python
import base64
raw_sig = hmac.new(
    base64.b64decode(self._api_secret),
    message.encode("utf-8"),
    digestmod=hashlib.sha256,
).digest()
signature = base64.b64encode(raw_sig).decode("utf-8")
```

**Better solution:** Use `py-clob-client==0.28.0` — handles auth correctly.
Pin to 0.28.0 specifically; newer versions have known 401 bugs (see
py-clob-client issue #187).

---

## 6. API Credentials — Wrong Keys

**Symptom:** 401 even after fixing HMAC — `Invalid api key`

**Root cause:**
Polymarket API keys (key, secret, passphrase) are **not arbitrary** — they
must be derived from your private key via Polymarket's auth endpoint.
Manually entered or guessed keys will always fail.

**Fix:** Use `py-clob-client` to derive credentials:
```python
from py_clob_client.client import ClobClient
client = ClobClient("https://clob.polymarket.com", key=private_key, chain_id=137)
creds = client.create_or_derive_api_creds()
# creds.api_key, creds.api_secret, creds.api_passphrase
```

Run this once locally and save the output to Railway env vars.

---

## 7. Geo-Block — 403 Forbidden

**Symptom:**
```
403 Forbidden — Trading restricted in your region
```

**Root cause:**
Polymarket blocks order placement from US/EU datacenter IPs (Railway
defaults to US East). Market data fetches still work; only POST /order
is blocked.

**Solutions (either works):**
1. Change Railway region to `asia-southeast1` (Singapore)
2. Set `PROXY_URL` env var pointing to a residential proxy (Webshare.io)

**Important:** `py-clob-client` uses the `requests` library internally and
ignores httpx proxy settings. Must set environment variables:
```python
os.environ["HTTP_PROXY"] = proxy_url
os.environ["HTTPS_PROXY"] = proxy_url
```
Set these **before** initializing `ClobClient`.

---

## 8. Zero Balance Error

**Symptom:**
```
400 — not enough balance/allowance: balance: 0, order amount: 9996000
```

**Root cause:**
Polymarket creates a **proxy wallet** (`0x3d77...`) separate from the
MetaMask EOA (`0xA369...`). The CLOB checks the proxy wallet's balance.
If ClobClient is initialized without the `funder` parameter, it uses
the EOA address which has $0.

**Fix:** Pass the proxy wallet address as `funder` with `signature_type=2`:
```python
self._clob = ClobClient(
    CLOB_BASE,
    key=private_key,
    chain_id=137,
    creds=creds,
    funder=proxy_wallet_address,   # The 0x3d77... address shown on Polymarket
    signature_type=2,              # POLY_GNOSIS_SAFE for proxy wallets
)
```

Add `POLYMARKET_PROXY_WALLET` as a Railway env var containing the proxy
wallet address (visible on polymarket.com when connected).

---

## 9. Gamma API Price Lag — Missing Sniper Windows

**Symptom:** Bot tracks 16 markets but 0 qualify despite markets visibly
resolving on the Polymarket website.

**Root cause:**
Gamma API `bestAsk` lags real-time CLOB prices by 30-60 seconds. By the
time Gamma shows 0.99, the market has already resolved and been pruned.

**Fix:** For markets within 3 minutes of expiry, fetch live price from the
CLOB `/midpoint` endpoint instead:
```python
if market.seconds_to_expiry <= 180:
    resp = await client.get(
        "https://clob.polymarket.com/midpoint",
        params={"token_id": market.yes_token_id}
    )
    mid = float(resp.json().get("mid", 0))
    if mid > 0:
        market.yes_price = mid
```

---

## 10. Only Trading UP Token — Halved Opportunities

**Symptom:** Bot rarely fires even when markets are clearly decided.

**Root cause:**
Each market has two tokens: UP and DOWN. If BTC goes down strongly,
the DOWN token hits 0.99 but the UP token is at 0.01. The bot was only
checking `yes_price` (UP token), missing all DOWN opportunities.

**Fix:** Always check both sides and buy whichever is winning:
```python
@property
def best_trade_side(self) -> tuple:
    no_price = round(1.0 - self.yes_price, 4)
    if no_price > self.yes_price:
        return ('no', no_price)
    return ('yes', self.yes_price)

@property
def trade_token_id(self) -> str:
    side, _ = self.best_trade_side
    return self.no_token_id if side == 'no' else self.yes_token_id
```

This doubles trade opportunities instantly.

---

## 11. Order Price Above CLOB Maximum

**Symptom:**
```
400 — price (0.995), min: 0.01 - max: 0.99
```

**Root cause:**
CLOB mid-price can return values above 0.99 (e.g. 0.995) when the market
is nearly resolved. Polymarket's CLOB only accepts prices in [0.01, 0.99].

**Fix:** Always cap order price at 0.99:
```python
price = min(price, 0.99)
```

---

## 12. Sniper Orders Cancelled After 5 Seconds

**Symptom:** Order placed successfully then immediately cancelled.

**Root cause:**
`sniper.order_timeout_seconds` was set to 5 in config. The order manager
cancels any order older than the timeout. At 0.99 with 60s left, 5 seconds
is far too short to fill.

**Fix:** Set sniper timeout to 55 seconds in `config.yaml`:
```yaml
sniper:
  order_timeout_seconds: 55
```

---

## 13. Auto-Redeem Never Fires

**Symptom:** Positions marked as filled but never redeemed.

**Root cause:**
The resolution watcher checked `poly_listener.get_market(market_id)` to
see if a market was expired. But expired markets are pruned from the
listener's tracking dict — so `get_market` returned `None` and the
redeem check never passed.

**Fix:** Use the expiry timestamp stored on the position itself:
```python
# Before (broken):
market = self.poly_listener.get_market(pos.market.market_id)
if market and market.is_expired:
    ...

# After (working):
if pos.market.is_expired:
    await self._attempt_redeem(pos)
```

---

## Quick Reference — Railway Environment Variables

| Variable | Description |
|---|---|
| `POLYMARKET_PRIVATE_KEY` | MetaMask private key (0x..., 64 hex chars) |
| `POLYMARKET_API_KEY` | Derived via py-clob-client |
| `POLYMARKET_API_SECRET` | Derived via py-clob-client |
| `POLYMARKET_API_PASSPHRASE` | Derived via py-clob-client |
| `POLYMARKET_PROXY_WALLET` | Proxy wallet shown on polymarket.com (0x3d77...) |
| `POLYMARKET_RELAYER_API_KEY` | From Polymarket Settings > Relayer API Keys |
| `PROXY_URL` | Residential proxy URL (Webshare.io) for geo-block bypass |
| `ANTHROPIC_API_KEY` | For AI trading agent on dashboard |

## Recommended Dependencies

```
py-clob-client==0.28.0   # Pin to 0.28.0 — newer versions have auth bugs
httpx==0.27.0
fastapi==0.111.0
uvicorn[standard]==0.30.1
pyyaml==6.0.1
python-dotenv==1.0.1
anthropic>=0.40.0
```

Do NOT pin `eth-account` — let `py-clob-client` resolve the version.
