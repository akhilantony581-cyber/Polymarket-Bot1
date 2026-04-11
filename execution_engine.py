"""
execution_engine.py
Handles order placement, cancellation, and redemption
via Polymarket CLOB — uses py-clob-client for signing/auth.
"""

import logging
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import httpx
from eth_account import Account
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType

logger = logging.getLogger(__name__)

CLOB_BASE = "https://clob.polymarket.com"
CHAIN_ID = 137


class OrderStatus(Enum):
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass
class PlacedOrder:
    order_id: str
    market_id: str
    token_id: str
    side: str
    price: float
    size: float
    filled_size: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    placed_at: float = field(default_factory=time.time)
    last_updated: float = field(default_factory=time.time)
    mode: str = "standard"

    @property
    def remaining_size(self) -> float:
        return self.size - self.filled_size

    @property
    def is_active(self) -> bool:
        return self.status in (OrderStatus.PENDING, OrderStatus.OPEN,
                               OrderStatus.PARTIALLY_FILLED)

    @property
    def age_seconds(self) -> float:
        return time.time() - self.placed_at


class ExecutionEngine:
    """Polymarket CLOB order execution via py-clob-client."""

    def __init__(self, config: dict):
        self.config = config
        self._private_key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
        self._api_key = os.environ.get("POLYMARKET_API_KEY", "")
        self._api_secret = os.environ.get("POLYMARKET_API_SECRET", "")
        self._api_passphrase = os.environ.get("POLYMARKET_API_PASSPHRASE", "")
        self._wallet_address = ""
        self._clob: Optional[ClobClient] = None

        proxy_url = os.environ.get("PROXY_URL", "")
        self._http = httpx.AsyncClient(
            timeout=10.0,
            proxy=proxy_url if proxy_url else None,
        )
        # Separate client for Polygon RPC — must NOT go through trading proxy
        self._rpc_http = httpx.AsyncClient(timeout=10.0)
        if proxy_url:
            logger.info(f"ExecutionEngine using proxy: {proxy_url[:30]}...")

        self._init_client()

    def _init_client(self):
        if not self._private_key:
            logger.warning("POLYMARKET_PRIVATE_KEY not set — order placement disabled")
            return
        # py-clob-client uses requests internally — set proxy via env vars
        proxy_url = os.environ.get("PROXY_URL", "")
        if proxy_url:
            os.environ.setdefault("HTTP_PROXY", proxy_url)
            os.environ.setdefault("HTTPS_PROXY", proxy_url)
        try:
            account = Account.from_key(self._private_key)
            self._wallet_address = account.address
            creds = ApiCreds(
                api_key=self._api_key,
                api_secret=self._api_secret,
                api_passphrase=self._api_passphrase,
            )
            proxy_wallet = os.environ.get("POLYMARKET_PROXY_WALLET", "")
            self._clob = ClobClient(
                CLOB_BASE,
                key=self._private_key,
                chain_id=CHAIN_ID,
                creds=creds,
                funder=proxy_wallet if proxy_wallet else self._wallet_address,
                signature_type=2 if proxy_wallet else 0,
            )
            logger.info(f"ExecutionEngine ready. Wallet: {self._wallet_address} Funder: {proxy_wallet or self._wallet_address}")
        except Exception as e:
            logger.error(f"Account init failed: {e}")

    # ------------------------------------------------------------------
    # LIVE USDC BALANCE (Polygon RPC — USDC.e balanceOf proxy wallet)
    # ------------------------------------------------------------------
    _balance_cache: Optional[float] = None
    _balance_cache_at: float = 0.0
    _BALANCE_TTL = 30.0  # seconds between RPC fetches

    async def get_usdc_balance(self) -> Optional[float]:
        """Return cached USDC balance; refresh via Polygon RPC every 30s."""
        now = time.time()
        if self._balance_cache is not None and now - self._balance_cache_at < self._BALANCE_TTL:
            return self._balance_cache
        try:
            wallet = os.environ.get("POLYMARKET_PROXY_WALLET", "") or self._wallet_address
            if not wallet:
                return None
            # USDC.e on Polygon (bridged USDC used by Polymarket)
            USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
            # balanceOf(address) ABI selector
            data = "0x70a08231" + wallet.lower().replace("0x", "").zfill(64)
            rpc_url = os.environ.get("POLYGON_RPC_URL", "https://polygon-rpc.com")
            resp = await self._rpc_http.post(rpc_url, json={
                "jsonrpc": "2.0", "id": 1,
                "method": "eth_call",
                "params": [{"to": USDC_E, "data": data}, "latest"],
            })
            raw = int(resp.json().get("result", "0x0"), 16)
            balance = raw / 1_000_000  # USDC has 6 decimals
            self._balance_cache = balance
            self._balance_cache_at = now
            return balance
        except Exception as e:
            logger.debug(f"USDC balance fetch failed: {e}")
            return self._balance_cache  # return stale if available

    # ------------------------------------------------------------------
    # PLACE LIMIT ORDER (BUY)
    # ------------------------------------------------------------------
    async def place_limit_order(
        self,
        token_id: str,
        market_id: str,
        price: float,
        size: float,
        mode: str = "standard",
    ) -> Optional[PlacedOrder]:
        if not self._clob:
            logger.error("No CLOB client — order placement disabled")
            return None

        try:
            price = round(price, 2)  # Polymarket tick size = $0.01
            shares = round(size / price, 6)
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=shares,
                side="BUY",
            )
            signed_order = self._clob.create_order(order_args)
            resp = self._clob.post_order(signed_order, OrderType.GTC)
            order_id = resp.get("orderID") or resp.get("order_id", "")
            if not order_id:
                logger.warning(f"No order ID in response: {resp}")
                return None

            order = PlacedOrder(
                order_id=order_id,
                market_id=market_id,
                token_id=token_id,
                side="buy",
                price=price,
                size=shares,
                mode=mode,
            )
            logger.info(
                f"Order placed [{mode}] {order_id} "
                f"price={price} shares={shares:.4f} market={market_id[:16]}..."
            )
            return order
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return None

    # ------------------------------------------------------------------
    # PLACE MARKET ORDER (FOK — Fill or Kill) for Snipe 2
    # ------------------------------------------------------------------
    async def place_market_order(
        self,
        token_id: str,
        market_id: str,
        size: float,
        min_price: float = 0.95,  # unused, kept for call-site compatibility
    ) -> Optional[PlacedOrder]:
        """
        Submit a Fill-or-Kill order at price=0.99 (highest valid tick).
        Fills immediately at the best available ask or cancels instantly.
        """
        if not self._clob:
            logger.error("No CLOB client — order placement disabled")
            return None

        price = 0.99  # highest valid tick — FOK fills at best ask up to this price
        try:
            shares = round(size / price, 6)
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=shares,
                side="BUY",
            )
            signed_order = self._clob.create_order(order_args)
            resp = self._clob.post_order(signed_order, OrderType.FOK)
            order_id = resp.get("orderID") or resp.get("order_id", "")
            if not order_id:
                logger.warning(f"SNIPE2 FOK no order ID: {resp}")
                return None

            order = PlacedOrder(
                order_id=order_id,
                market_id=market_id,
                token_id=token_id,
                side="buy",
                price=price,
                size=shares,
                mode="snipe2",
            )
            logger.info(
                f"SNIPE2 FOK placed {order_id} "
                f"shares={shares:.4f} market={market_id[:16]}..."
            )
            return order
        except Exception as e:
            logger.error(f"SNIPE2 market order failed: {e}")
            return None

    # ------------------------------------------------------------------
    # PLACE MAKER ORDER (FOK) — fills immediately or cancels, no resting in book
    # Returns (PlacedOrder, fill_price) or (None, None)
    # ------------------------------------------------------------------
    async def place_maker_order(
        self,
        token_id: str,
        market_id: str,
        price: float,          # the target price (e.g. win_price)
        size: float,           # USDC amount
        min_fill_price: float, # abort if fill price drops below this
        mode: str = "maker",
    ):
        """
        FOK order for market maker win side.
        - Uses FOK so it fills at current ask or cancels — never rests in book.
        - Checks fresh midpoint before submitting; aborts if below min_fill_price.
        - Returns (PlacedOrder, actual_price) or (None, None).
        """
        if not self._clob:
            logger.error("No CLOB client")
            return None, None

        # Fresh price check — reject if market has moved away
        try:
            resp = await self._http.get(f"{CLOB_BASE}/midpoint", params={"token_id": token_id})
            fresh_mid = float(resp.json().get("mid", 0))
            if fresh_mid > 0 and fresh_mid < min_fill_price:
                logger.warning(
                    f"MAKER aborted: fresh midpoint {fresh_mid:.4f} < min_fill_price {min_fill_price:.4f}"
                )
                return None, None
        except Exception as e:
            logger.debug(f"MAKER pre-flight check failed: {e}")

        try:
            shares = round(size / price, 6)
            order_args = OrderArgs(token_id=token_id, price=price, size=shares, side="BUY")
            signed_order = self._clob.create_order(order_args)
            resp = self._clob.post_order(signed_order, OrderType.FOK)
            order_id = resp.get("orderID") or resp.get("order_id", "")
            if not order_id:
                logger.info(f"MAKER FOK did not fill (no order ID): {resp}")
                return None, None

            # Estimate actual fill price from response if available, else use posted price
            fill_price = float(resp.get("price", price))

            order = PlacedOrder(
                order_id=order_id,
                market_id=market_id,
                token_id=token_id,
                side="buy",
                price=fill_price,
                size=shares,
                mode=mode,
            )
            logger.info(
                f"MAKER FOK filled {order_id} price={fill_price:.4f} "
                f"shares={shares:.4f} market={market_id[:16]}..."
            )
            return order, fill_price
        except Exception as e:
            logger.error(f"MAKER order failed: {e}")
            return None, None

    # ------------------------------------------------------------------
    # CANCEL ORDER
    # ------------------------------------------------------------------
    async def cancel_order(self, order: PlacedOrder) -> bool:
        if not self._clob:
            return False
        if not order.is_active:
            return True
        try:
            self._clob.cancel(order.order_id)
            order.status = OrderStatus.CANCELLED
            order.last_updated = time.time()
            logger.info(f"Order cancelled: {order.order_id}")
            return True
        except Exception as e:
            logger.error(f"Cancel failed for {order.order_id}: {e}")
            return False

    # ------------------------------------------------------------------
    # MANUAL EXIT (SELL)
    # ------------------------------------------------------------------
    async def manual_exit(
        self,
        token_id: str,
        market_id: str,
        size: float,
        exit_price: float,
    ) -> Optional[PlacedOrder]:
        if not self._clob:
            return None
        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=exit_price,
                size=size,
                side="SELL",
            )
            signed_order = self._clob.create_order(order_args)
            resp = self._clob.post_order(signed_order, OrderType.GTC)
            order_id = resp.get("orderID", "")
            if not order_id:
                return None
            order = PlacedOrder(
                order_id=order_id,
                market_id=market_id,
                token_id=token_id,
                side="sell",
                price=exit_price,
                size=size,
                mode="manual_exit",
            )
            logger.info(f"Manual exit placed: {order_id} @ {exit_price}")
            return order
        except Exception as e:
            logger.error(f"Manual exit failed: {e}")
            return None

    # ------------------------------------------------------------------
    # REPRICE (sniper)
    # ------------------------------------------------------------------
    async def reprice_order(
        self,
        order: PlacedOrder,
        new_price: float,
        token_id: str,
        market_id: str,
    ) -> Optional[PlacedOrder]:
        if not await self.cancel_order(order):
            return None
        remaining = order.remaining_size
        if remaining <= 0:
            return None
        return await self.place_limit_order(
            token_id=token_id,
            market_id=market_id,
            price=round(new_price, 2),
            size=remaining * new_price,
            mode="sniper",
        )

    # ------------------------------------------------------------------
    # ORDER STATUS
    # ------------------------------------------------------------------
    async def get_order_status(self, order: PlacedOrder) -> OrderStatus:
        if not self._clob:
            return order.status
        try:
            data = self._clob.get_order(order.order_id)
            filled = float(data.get("size_matched", 0))
            order.filled_size = filled
            order.last_updated = time.time()
            raw = data.get("status", "").lower()
            order.status = {
                "live":      OrderStatus.OPEN,
                "matched":   OrderStatus.FILLED,
                "cancelled": OrderStatus.CANCELLED,
            }.get(raw, OrderStatus.OPEN)
            return order.status
        except Exception as e:
            logger.debug(f"Status check failed: {e}")
            return order.status

    # ------------------------------------------------------------------
    # BEST ASK
    # ------------------------------------------------------------------
    async def get_best_ask(self, token_id: str) -> Optional[float]:
        try:
            resp = await self._http.get(
                f"{CLOB_BASE}/book", params={"token_id": token_id}
            )
            resp.raise_for_status()
            asks = resp.json().get("asks", [])
            if asks:
                return min(float(a["price"]) for a in asks)
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # REDEEM via Safe.execTransaction() directly on proxy wallet (0x3d77...)
    # EOA signs a Safe EIP-712 tx and submits it on-chain.
    # Costs ~$0.01 MATIC. No relayer needed.
    # ------------------------------------------------------------------
    async def redeem_position(self, condition_id: str, amounts: list) -> bool:
        if not condition_id:
            logger.error("redeem_position: condition_id is empty")
            return False

        proxy_wallet = os.environ.get("POLYMARKET_PROXY_WALLET", "")
        rpc_url      = os.environ.get("POLYGON_RPC_URL", "https://polygon-rpc.com")

        if not self._private_key or not proxy_wallet:
            logger.warning("Missing private key or proxy wallet — cannot redeem")
            return False

        try:
            from eth_abi import encode as abi_encode
            from eth_utils import keccak, to_checksum_address

            CTF    = to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")
            USDC_E = to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
            ZERO   = "0x0000000000000000000000000000000000000000"
            safe   = to_checksum_address(proxy_wallet)

            # ── 1. Encode redeemPositions calldata
            cid_bytes = bytes.fromhex(condition_id.replace("0x", "").zfill(64))
            redeem_selector = keccak(text="redeemPositions(address,bytes32,bytes32,uint256[])")[:4]
            redeem_calldata = redeem_selector + abi_encode(
                ["address", "bytes32", "bytes32", "uint256[]"],
                [USDC_E, b"\x00" * 32, cid_bytes, [1, 2]],
            )

            # ── 2. Get Safe nonce
            nonce_resp = await self._rpc_http.post(rpc_url, json={
                "jsonrpc": "2.0", "method": "eth_call",
                "params": [{"to": safe, "data": "0xaffed0e0"}, "latest"], "id": 1,
            }, timeout=8.0)
            safe_nonce = int(nonce_resp.json().get("result", "0x0"), 16)

            # ── 3. Build Safe EIP-712 digest (Safe v1.3.0, chainId=137)
            domain_typehash = keccak(text="EIP712Domain(uint256 chainId,address verifyingContract)")
            domain_sep = keccak(abi_encode(["bytes32", "uint256", "address"], [domain_typehash, 137, safe]))

            safe_tx_typehash = keccak(
                text="SafeTx(address to,uint256 value,bytes data,uint8 operation,"
                     "uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,"
                     "address gasToken,address refundReceiver,uint256 nonce)"
            )
            safe_tx_hash = keccak(abi_encode(
                ["bytes32","address","uint256","bytes32","uint8","uint256","uint256","uint256","address","address","uint256"],
                [safe_tx_typehash, CTF, 0, keccak(redeem_calldata), 0, 0, 0, 0, ZERO, ZERO, safe_nonce],
            ))
            digest = keccak(b"\x19\x01" + domain_sep + safe_tx_hash)

            # ── 4. Sign digest with EOA key (Safe owner)
            from eth_keys import keys as eth_keys_lib
            pk  = eth_keys_lib.PrivateKey(bytes.fromhex(self._private_key.replace("0x", "")))
            sig = pk.sign_msg_hash(digest)
            v   = sig.v + 27
            signature = sig.r.to_bytes(32, "big") + sig.s.to_bytes(32, "big") + bytes([v])

            # ── 5. Encode execTransaction calldata
            exec_selector = keccak(
                text="execTransaction(address,uint256,bytes,uint8,uint256,uint256,uint256,address,address,bytes)"
            )[:4]
            exec_calldata = exec_selector + abi_encode(
                ["address","uint256","bytes","uint8","uint256","uint256","uint256","address","address","bytes"],
                [CTF, 0, redeem_calldata, 0, 0, 0, 0, ZERO, ZERO, signature],
            )

            # ── 6. Get EOA nonce for the outer tx
            eoa_nonce_resp = await self._rpc_http.post(rpc_url, json={
                "jsonrpc": "2.0", "method": "eth_getTransactionCount",
                "params": [self._wallet_address, "latest"], "id": 2,
            }, timeout=8.0)
            eoa_nonce_result = eoa_nonce_resp.json()
            if "error" in eoa_nonce_result:
                logger.error(f"RPC error: {eoa_nonce_result['error']}")
                return False
            eoa_nonce = int(eoa_nonce_result.get("result", "0x0"), 16)

            # ── 7. Send execTransaction TO the Safe (proxy wallet)
            tx = {
                "to":       safe,
                "data":     "0x" + exec_calldata.hex(),
                "nonce":    eoa_nonce,
                "chainId":  137,
                "gasPrice": 200_000_000_000,
                "gas":      300_000,
                "value":    0,
            }
            signed  = Account.sign_transaction(tx, self._private_key)
            raw_tx  = signed.raw_transaction if hasattr(signed, "raw_transaction") else signed.rawTransaction
            raw_hex = "0x" + raw_tx.hex()

            send_resp = await self._rpc_http.post(rpc_url, json={
                "jsonrpc": "2.0", "method": "eth_sendRawTransaction",
                "params": [raw_hex], "id": 3,
            }, timeout=15.0)
            result = send_resp.json()

            if "result" not in result or not result["result"]:
                logger.error(f"Redeem tx failed: {result.get('error', result)}")
                return None

            tx_hash = result["result"]
            logger.info(f"Redeem tx sent: {tx_hash} condition={condition_id[:16]}...")

            # Wait for receipt and parse USDC Transfer log to get actual proceeds
            usdc_received = await self._wait_redeem_receipt(tx_hash, proxy_wallet, rpc_url)
            logger.info(f"Redeem proceeds: {usdc_received:.4f} USDC condition={condition_id[:16]}")
            return usdc_received

        except Exception as e:
            logger.error(f"Redeem exception: {e}", exc_info=True)
            return None

    async def _wait_redeem_receipt(self, tx_hash: str, wallet: str, rpc_url: str) -> float:
        """
        Poll for tx receipt and sum USDC Transfer(to=wallet) log amounts.
        Returns USDC received (0.0 for a losing redeem, >0 for a win).
        """
        USDC_E = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
        # ERC-20 Transfer(address indexed from, address indexed to, uint256 value)
        TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        wallet_padded = "0x" + wallet.lower().replace("0x", "").zfill(64)

        for _ in range(15):  # up to ~15s
            await asyncio.sleep(1.0)
            try:
                resp = await self._rpc_http.post(rpc_url, json={
                    "jsonrpc": "2.0", "method": "eth_getTransactionReceipt",
                    "params": [tx_hash], "id": 4,
                }, timeout=8.0)
                receipt = resp.json().get("result")
                if not receipt:
                    continue  # not mined yet

                total = 0.0
                for log in receipt.get("logs", []):
                    if (log.get("address", "").lower() == USDC_E
                            and log.get("topics", [None, None, None])[2] == wallet_padded
                            and log.get("topics", [None])[0] == TRANSFER_TOPIC):
                        # USDC has 6 decimals
                        raw = int(log["data"], 16)
                        total += raw / 1_000_000
                return total
            except Exception as e:
                logger.debug(f"Receipt poll error: {e}")

        logger.warning(f"Redeem receipt not found after 15s for {tx_hash}")
        return 0.0

    async def close(self):
        if self._http:
            await self._http.aclose()
        if self._rpc_http:
            await self._rpc_http.aclose()
