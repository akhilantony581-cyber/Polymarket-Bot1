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
        if mode != "manual" and price < self.config.get("price", {}).get("min_entry", 0.98):
            logger.warning(f"Rejected: price {price} below hard floor")
            return None
        if not self._clob:
            logger.error("No CLOB client — order placement disabled")
            return None

        try:
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
            price=new_price,
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
    # REDEEM via Polymarket Relayer v2 (gasless Safe transaction)
    # Docs: https://docs.polymarket.com/developers/builders/relayer-client
    # ------------------------------------------------------------------
    async def redeem_position(self, condition_id: str, amounts: list) -> bool:
        if not condition_id:
            logger.error("redeem_position: condition_id is empty — cannot redeem")
            return False

        relayer_key = os.environ.get("POLYMARKET_RELAYER_API_KEY", "")
        proxy_wallet = os.environ.get("POLYMARKET_PROXY_WALLET", "")
        rpc_url = os.environ.get("POLYGON_RPC_URL", "https://polygon-rpc.com")

        if not relayer_key:
            logger.warning("POLYMARKET_RELAYER_API_KEY not set — cannot auto-redeem. Redeem manually on polymarket.com")
            return False
        if not proxy_wallet:
            logger.warning("POLYMARKET_PROXY_WALLET not set — cannot auto-redeem")
            return False

        try:
            from web3 import Web3
            from eth_abi import encode as abi_encode
            from eth_keys import keys as eth_keys

            w3 = Web3()
            CTF      = Web3.to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")
            USDC_E   = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
            ZERO_ADDR = "0x0000000000000000000000000000000000000000"
            safe_addr = Web3.to_checksum_address(proxy_wallet)

            # ── 1. Encode redeemPositions(collateral, parentCollectionId, conditionId, indexSets)
            cid_bytes = bytes.fromhex(condition_id.replace("0x", "").zfill(64))
            fn_selector = w3.keccak(text="redeemPositions(address,bytes32,bytes32,uint256[])")[:4]
            calldata = fn_selector + abi_encode(
                ["address", "bytes32", "bytes32", "uint256[]"],
                [USDC_E, b"\x00" * 32, cid_bytes, [1, 2]],
            )

            # ── 2. Fetch Safe nonce via RPC (nonce() selector = 0xaffed0e0)
            nonce_resp = await self._http.post(rpc_url, json={
                "jsonrpc": "2.0", "method": "eth_call",
                "params": [{"to": safe_addr, "data": "0xaffed0e0"}, "latest"],
                "id": 1,
            }, timeout=5.0)
            safe_nonce = int(nonce_resp.json().get("result", "0x0"), 16)

            # ── 3. Build EIP-712 Safe transaction digest
            # Safe v1.3.0 domain includes chainId
            domain_typehash = w3.keccak(text="EIP712Domain(uint256 chainId,address verifyingContract)")
            domain_separator = w3.keccak(
                abi_encode(["bytes32", "uint256", "address"], [domain_typehash, 137, safe_addr])
            )

            safe_tx_typehash = w3.keccak(
                text="SafeTx(address to,uint256 value,bytes data,uint8 operation,"
                     "uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,"
                     "address gasToken,address refundReceiver,uint256 nonce)"
            )
            safe_tx_hash = w3.keccak(abi_encode(
                ["bytes32", "address", "uint256", "bytes32", "uint8",
                 "uint256", "uint256", "uint256", "address", "address", "uint256"],
                [safe_tx_typehash, CTF, 0, w3.keccak(calldata),
                 0, 0, 0, 0, ZERO_ADDR, ZERO_ADDR, safe_nonce],
            ))

            # 0x1901 || domainSeparator || safeTxHash
            digest = w3.keccak(b"\x19\x01" + domain_separator + safe_tx_hash)

            # ── 4. Sign the raw digest (no Ethereum prefix — EIP-712 already encoded it)
            pk = eth_keys.PrivateKey(bytes.fromhex(self._private_key.replace("0x", "")))
            sig = pk.sign_msg_hash(bytes(digest))
            v = sig.v + 27  # Gnosis Safe expects v=27 or v=28
            signature = "0x" + sig.r.to_bytes(32, "big").hex() + sig.s.to_bytes(32, "big").hex() + bytes([v]).hex()

            # ── 5. Submit to Relayer v2
            account = Account.from_key(self._private_key)
            payload = {
                "from":        account.address,
                "to":          CTF,
                "proxyWallet": safe_addr,
                "data":        "0x" + calldata.hex(),
                "nonce":       str(safe_nonce),
                "signature":   signature,
                "type":        "PROXY",
                "signatureParams": {
                    "gasPrice":       "0",
                    "operation":      "0",
                    "safeTxnGas":     "0",
                    "baseGas":        "0",
                    "gasToken":       ZERO_ADDR,
                    "refundReceiver": ZERO_ADDR,
                },
            }
            logger.info(
                f"Redeem submit: condition={condition_id[:16]}... "
                f"nonce={safe_nonce} safe={safe_addr[:10]}..."
            )
            resp = await self._http.post(
                "https://relayer-v2.polymarket.com/submit",
                json=payload,
                headers={
                    "RELAYER_API_KEY":         relayer_key,
                    "RELAYER_API_KEY_ADDRESS":  safe_addr,
                    "Content-Type":             "application/json",
                },
                timeout=15.0,
            )
            body = resp.text[:400]
            if resp.status_code in (200, 201, 202):
                logger.info(f"Redeem accepted by Relayer v2: {body}")
                return True
            else:
                logger.error(f"Relayer v2 HTTP {resp.status_code}: {body}")
                return False

        except Exception as e:
            logger.error(f"Relayer redeem exception: {e}", exc_info=True)
            return False

    async def close(self):
        if self._http:
            await self._http.aclose()
