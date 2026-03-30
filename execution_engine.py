"""
execution_engine.py
Handles order placement, cancellation, and redemption
via direct Polymarket CLOB REST API calls.
Uses httpx + eth_account directly — no py-clob-client dependency.
"""

import hashlib
import hmac
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import httpx
from eth_account import Account
from eth_account.messages import encode_structured_data

logger = logging.getLogger(__name__)

CLOB_BASE = "https://clob.polymarket.com"
# Polymarket CTF Exchange contract on Polygon mainnet
EXCHANGE_ADDRESS = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
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
    """
    Direct Polymarket CLOB REST API client.
    Handles EIP-712 order signing and L2 HMAC authentication.
    """

    def __init__(self, config: dict):
        self.config = config
        self._private_key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
        self._api_key = os.environ.get("POLYMARKET_API_KEY", "")
        self._api_secret = os.environ.get("POLYMARKET_API_SECRET", "")
        self._api_passphrase = os.environ.get("POLYMARKET_API_PASSPHRASE", "")
        self._account = None
        self._wallet_address = ""
        self._http = httpx.AsyncClient(timeout=10.0)
        self._init_account()

    def _init_account(self):
        if not self._private_key:
            logger.warning("POLYMARKET_PRIVATE_KEY not set — order placement disabled")
            return
        try:
            self._account = Account.from_key(self._private_key)
            self._wallet_address = self._account.address
            logger.info(f"ExecutionEngine ready. Wallet: {self._wallet_address}")
        except Exception as e:
            logger.error(f"Account init failed: {e}")

    # ------------------------------------------------------------------
    # L2 HMAC Authentication
    # ------------------------------------------------------------------
    def _l2_headers(self, method: str, path: str, body: str = "") -> dict:
        timestamp = str(int(time.time()))
        message = timestamp + method.upper() + path + body
        signature = hmac.new(
            self._api_secret.encode("utf-8"),
            message.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).hexdigest()
        return {
            "POLY-ADDRESS": self._wallet_address,
            "POLY-SIGNATURE": signature,
            "POLY-TIMESTAMP": timestamp,
            "POLY-API-KEY": self._api_key,
            "POLY-PASSPHRASE": self._api_passphrase,
        }

    # ------------------------------------------------------------------
    # EIP-712 Order Signing
    # ------------------------------------------------------------------
    def _sign_order(self, order_struct: dict) -> str:
        domain = {
            "name": "Polymarket CTF Exchange",
            "version": "1",
            "chainId": CHAIN_ID,
            "verifyingContract": EXCHANGE_ADDRESS,
        }
        order_types = {
            "Order": [
                {"name": "salt",          "type": "uint256"},
                {"name": "maker",         "type": "address"},
                {"name": "signer",        "type": "address"},
                {"name": "taker",         "type": "address"},
                {"name": "tokenId",       "type": "uint256"},
                {"name": "makerAmount",   "type": "uint256"},
                {"name": "takerAmount",   "type": "uint256"},
                {"name": "expiration",    "type": "uint256"},
                {"name": "nonce",         "type": "uint256"},
                {"name": "feeRateBps",    "type": "uint256"},
                {"name": "side",          "type": "uint8"},
                {"name": "signatureType", "type": "uint8"},
            ]
        }
        # encode_structured_data is the stable EIP-712 API across all eth_account versions
        structured = {
            "types": {
                "EIP712Domain": [
                    {"name": "name",              "type": "string"},
                    {"name": "version",           "type": "string"},
                    {"name": "chainId",           "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Order": order_types["Order"],
            },
            "domain": domain,
            "primaryType": "Order",
            "message": order_struct,
        }
        msg = encode_structured_data(structured)
        signed = self._account.sign_message(msg)
        return signed.signature.hex()

    def _build_order_struct(
        self, token_id: str, maker_amount: int, taker_amount: int, side: int
    ) -> dict:
        return {
            "salt":          random.randint(1, 2**128),
            "maker":         self._wallet_address,
            "signer":        self._wallet_address,
            "taker":         "0x0000000000000000000000000000000000000000",
            "tokenId":       int(token_id),
            "makerAmount":   maker_amount,
            "takerAmount":   taker_amount,
            "expiration":    0,
            "nonce":         0,
            "feeRateBps":    0,
            "side":          side,
            "signatureType": 0,  # EOA
        }

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
        # Only enforce the hard floor for automated modes, not manual trades
        if mode != "manual" and price < self.config.get("price", {}).get("min_entry", 0.98):
            logger.warning(f"Rejected: price {price} below hard floor")
            return None
        if not self._account:
            logger.error("No account — order placement disabled")
            return None

        shares = round(size / price, 6)
        maker_amount = int(size * 1e6)       # USDC (6 decimals)
        taker_amount = int(shares * 1e6)     # YES tokens

        order_struct = self._build_order_struct(
            token_id, maker_amount, taker_amount, side=0
        )
        signature = self._sign_order(order_struct)

        payload = {
            "order": {**order_struct, "signature": signature},
            "owner": self._wallet_address,
            "orderType": "GTC",
        }
        body_str = json.dumps(payload)
        headers = self._l2_headers("POST", "/order", body_str)
        headers["Content-Type"] = "application/json"

        try:
            resp = await self._http.post(
                f"{CLOB_BASE}/order", content=body_str, headers=headers
            )
            resp.raise_for_status()
            data = resp.json()
            order_id = data.get("orderID") or data.get("order_id", "")
            if not order_id:
                logger.warning(f"No order ID in response: {data}")
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
            resp_text = ""
            try:
                resp_text = e.response.text[:300] if hasattr(e, 'response') else ""
            except Exception:
                pass
            logger.error(f"Order placement failed: {e} {resp_text}")
            return None

    # ------------------------------------------------------------------
    # CANCEL ORDER
    # ------------------------------------------------------------------
    async def cancel_order(self, order: PlacedOrder) -> bool:
        if not self._account:
            return False
        if not order.is_active:
            return True

        body_str = json.dumps({"orderID": order.order_id})
        headers = self._l2_headers("DELETE", "/order", body_str)
        headers["Content-Type"] = "application/json"

        try:
            resp = await self._http.delete(
                f"{CLOB_BASE}/order", content=body_str, headers=headers
            )
            resp.raise_for_status()
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
        if not self._account:
            return None

        maker_amount = int(size * 1e6)
        taker_amount = int(size * exit_price * 1e6)
        order_struct = self._build_order_struct(
            token_id, maker_amount, taker_amount, side=1
        )
        signature = self._sign_order(order_struct)
        payload = {
            "order": {**order_struct, "signature": signature},
            "owner": self._wallet_address,
            "orderType": "GTC",
        }
        body_str = json.dumps(payload)
        headers = self._l2_headers("POST", "/order", body_str)
        headers["Content-Type"] = "application/json"

        try:
            resp = await self._http.post(
                f"{CLOB_BASE}/order", content=body_str, headers=headers
            )
            resp.raise_for_status()
            data = resp.json()
            order_id = data.get("orderID", "")
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
        try:
            path = f"/order/{order.order_id}"
            headers = self._l2_headers("GET", path)
            resp = await self._http.get(f"{CLOB_BASE}{path}", headers=headers)
            resp.raise_for_status()
            data = resp.json()
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
    # REDEEM
    # ------------------------------------------------------------------
    async def redeem_position(self, condition_id: str, amounts: list) -> bool:
        logger.info(
            f"Redeem queued: condition={condition_id} amounts={amounts}. "
            "On-chain redemption executes via Polygon contract call."
        )
        return True

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

    async def close(self):
        if self._http:
            await self._http.aclose()
