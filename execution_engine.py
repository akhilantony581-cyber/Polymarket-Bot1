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
    # REDEEM via ProxyWalletFactory.proxy() — same approach as
    # 0xFives/Polymarket-Arbitrage-Crypto-Trading-Bot-V3
    # EOA calls factory which executes redeemPositions via proxy wallet.
    # Costs ~$0.01 in MATIC gas. No relayer needed.
    # ------------------------------------------------------------------
    async def redeem_position(self, condition_id: str, amounts: list) -> bool:
        if not condition_id:
            logger.error("redeem_position: condition_id is empty — cannot redeem")
            return False

        rpc_url = os.environ.get("POLYGON_RPC_URL", "https://polygon-rpc.com")

        if not self._private_key:
            logger.warning("No private key — cannot redeem")
            return False

        try:
            from eth_abi import encode as abi_encode
            from eth_utils import keccak, to_checksum_address

            PROXY_FACTORY = to_checksum_address("0xaB45c5A4B0c941a2F231C04C3f49182e1A254052")
            CTF           = to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")
            USDC_E        = to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")

            # ── 1. Encode redeemPositions(USDC_E, 0x0, conditionId, [1,2]) calldata
            cid_bytes = bytes.fromhex(condition_id.replace("0x", "").zfill(64))
            redeem_selector = keccak(text="redeemPositions(address,bytes32,bytes32,uint256[])")[:4]
            redeem_calldata = redeem_selector + abi_encode(
                ["address", "bytes32", "bytes32", "uint256[]"],
                [USDC_E, b"\x00" * 32, cid_bytes, [1, 2]],
            )

            # ── 2. Encode ProxyWalletFactory.proxy([(to, typeCode, data, value)])
            # struct Transaction { address to; uint8 typeCode; bytes data; uint256 value; }
            proxy_selector = keccak(text="proxy((address,uint8,bytes,uint256)[])")[:4]
            proxy_calldata = proxy_selector + abi_encode(
                ["(address,uint8,bytes,uint256)[]"],
                [[(CTF, 1, redeem_calldata, 0)]],
            )

            # ── 3. Get EOA nonce
            nonce_resp = await self._http.post(rpc_url, json={
                "jsonrpc": "2.0", "method": "eth_getTransactionCount",
                "params": [self._wallet_address, "latest"], "id": 1,
            }, timeout=8.0)
            eoa_nonce = int(nonce_resp.json()["result"], 16)

            # ── 4. Sign and send the transaction (EOA pays ~$0.01 in MATIC gas)
            tx = {
                "to":       PROXY_FACTORY,
                "data":     "0x" + proxy_calldata.hex(),
                "nonce":    eoa_nonce,
                "chainId":  137,
                "gasPrice": 200_000_000_000,  # 200 gwei
                "gas":      250_000,
                "value":    0,
            }
            signed = Account.sign_transaction(tx, self._private_key)
            raw_hex = "0x" + signed.rawTransaction.hex()

            send_resp = await self._http.post(rpc_url, json={
                "jsonrpc": "2.0", "method": "eth_sendRawTransaction",
                "params": [raw_hex], "id": 2,
            }, timeout=15.0)
            result = send_resp.json()

            if "result" in result and result["result"]:
                tx_hash = result["result"]
                logger.info(f"Redeem tx sent: {tx_hash} condition={condition_id[:16]}...")
                return True
            else:
                err = result.get("error", result)
                logger.error(f"Redeem tx failed: {err}")
                return False

        except Exception as e:
            logger.error(f"Redeem exception: {e}", exc_info=True)
            return False

    async def close(self):
        if self._http:
            await self._http.aclose()
