"""
execution_engine.py
Handles order placement, repricing, cancellation, and redemption
via the Polymarket CLOB API.
LIMIT ORDERS ONLY. No market orders ever.
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import httpx
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, Side

logger = logging.getLogger(__name__)


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
    side: str                   # "buy" or "sell"
    price: float
    size: float
    filled_size: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    placed_at: float = field(default_factory=time.time)
    last_updated: float = field(default_factory=time.time)
    mode: str = "standard"      # standard / sniper / maker

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
    Wraps the Polymarket CLOB client for order lifecycle management.
    Enforces limit-order-only policy.
    """

    def __init__(self, config: dict):
        self.config = config
        self._client: Optional[ClobClient] = None
        self._http: Optional[httpx.AsyncClient] = None
        self._init_client()

    def _init_client(self):
        try:
            private_key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
            api_key = os.environ.get("POLYMARKET_API_KEY", "")
            api_secret = os.environ.get("POLYMARKET_API_SECRET", "")
            api_passphrase = os.environ.get("POLYMARKET_API_PASSPHRASE", "")
            chain_id = int(os.environ.get("POLYGON_CHAIN_ID", "137"))

            self._client = ClobClient(
                host="https://clob.polymarket.com",
                chain_id=chain_id,
                key=private_key,
                creds={
                    "apiKey": api_key,
                    "secret": api_secret,
                    "passphrase": api_passphrase,
                }
            )
            self._http = httpx.AsyncClient(timeout=10.0)
            logger.info("ExecutionEngine initialized with CLOB client")
        except Exception as e:
            logger.error(f"Failed to initialize CLOB client: {e}")
            self._client = None

    # ------------------------------------------------------------------
    # PLACE LIMIT ORDER
    # ------------------------------------------------------------------
    async def place_limit_order(
        self,
        token_id: str,
        market_id: str,
        price: float,
        size: float,
        mode: str = "standard",
    ) -> Optional[PlacedOrder]:
        """
        Place a BUY limit order. Never places market orders.
        price: limit price (e.g. 0.982)
        size: USDC amount to spend
        """
        if price < self.config.get("price", {}).get("min_entry", 0.98):
            logger.warning(f"Order rejected: price {price} below min_entry hard floor")
            return None

        if not self._client:
            logger.error("CLOB client not initialized")
            return None

        # Size in shares = USDC / price
        shares = round(size / price, 4)

        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=shares,
                side=Side.BUY,
            )
            resp = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._client.create_and_post_order(order_args)
            )

            if not resp or not resp.get("orderID"):
                logger.warning(f"Order placement returned no ID: {resp}")
                return None

            order = PlacedOrder(
                order_id=resp["orderID"],
                market_id=market_id,
                token_id=token_id,
                side="buy",
                price=price,
                size=shares,
                mode=mode,
            )
            logger.info(
                f"Order placed [{mode}] ID={order.order_id} "
                f"price={price} shares={shares} market={market_id}"
            )
            return order

        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return None

    # ------------------------------------------------------------------
    # CANCEL ORDER
    # ------------------------------------------------------------------
    async def cancel_order(self, order: PlacedOrder) -> bool:
        if not self._client:
            return False
        if not order.is_active:
            return True

        try:
            resp = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._client.cancel(order_id=order.order_id)
            )
            if resp:
                order.status = OrderStatus.CANCELLED
                order.last_updated = time.time()
                logger.info(f"Order cancelled: {order.order_id}")
                return True
        except Exception as e:
            logger.error(f"Cancel failed for {order.order_id}: {e}")
        return False

    # ------------------------------------------------------------------
    # MANUAL EXIT (sell back position at limit)
    # ------------------------------------------------------------------
    async def manual_exit(
        self,
        token_id: str,
        market_id: str,
        size: float,
        exit_price: float,
    ) -> Optional[PlacedOrder]:
        """
        Place a SELL limit order to manually exit a filled position.
        Only called from dashboard/Telegram — never by bot logic automatically.
        """
        if not self._client:
            logger.error("CLOB client not initialized")
            return None

        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=exit_price,
                size=size,
                side=Side.SELL,
            )
            resp = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._client.create_and_post_order(order_args)
            )
            if not resp or not resp.get("orderID"):
                return None

            order = PlacedOrder(
                order_id=resp["orderID"],
                market_id=market_id,
                token_id=token_id,
                side="sell",
                price=exit_price,
                size=size,
                mode="manual_exit",
            )
            logger.info(f"Manual exit order placed: {order.order_id} @ {exit_price}")
            return order
        except Exception as e:
            logger.error(f"Manual exit failed: {e}")
            return None

    # ------------------------------------------------------------------
    # REPRICE (sniper mode)
    # ------------------------------------------------------------------
    async def reprice_order(
        self,
        order: PlacedOrder,
        new_price: float,
        token_id: str,
        market_id: str,
    ) -> Optional[PlacedOrder]:
        """Cancel existing order and place at new price. Used in sniper mode."""
        cancelled = await self.cancel_order(order)
        if not cancelled:
            logger.warning(f"Reprice: could not cancel {order.order_id}")
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
    # CHECK ORDER STATUS
    # ------------------------------------------------------------------
    async def get_order_status(self, order: PlacedOrder) -> OrderStatus:
        if not self._client:
            return order.status

        try:
            resp = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._client.get_order(order.order_id)
            )
            if not resp:
                return order.status

            raw_status = resp.get("status", "").lower()
            filled = float(resp.get("size_matched", 0))
            order.filled_size = filled
            order.last_updated = time.time()

            status_map = {
                "live": OrderStatus.OPEN,
                "matched": OrderStatus.FILLED,
                "cancelled": OrderStatus.CANCELLED,
            }
            order.status = status_map.get(raw_status, OrderStatus.OPEN)
            return order.status
        except Exception as e:
            logger.debug(f"Status check failed for {order.order_id}: {e}")
            return order.status

    # ------------------------------------------------------------------
    # REDEEM WINNING POSITION
    # ------------------------------------------------------------------
    async def redeem_position(self, condition_id: str, amounts: list[int]) -> bool:
        """
        Redeem settled YES tokens for USDC on Polygon.
        Called automatically after market resolution.
        """
        if not self._client:
            return False

        try:
            resp = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._client.redeem_positions(
                    condition_id=condition_id,
                    amounts=amounts,
                )
            )
            if resp:
                logger.info(f"Redeemed position for condition {condition_id}")
                return True
        except Exception as e:
            logger.error(f"Redeem failed for {condition_id}: {e}")
        return False

    # ------------------------------------------------------------------
    # GET BEST ASK (for sniper repricing)
    # ------------------------------------------------------------------
    async def get_best_ask(self, token_id: str) -> Optional[float]:
        if not self._http:
            return None
        try:
            resp = await self._http.get(
                "https://clob.polymarket.com/book",
                params={"token_id": token_id}
            )
            resp.raise_for_status()
            data = resp.json()
            asks = data.get("asks", [])
            if asks:
                return min(float(a["price"]) for a in asks)
        except Exception:
            pass
        return None

    async def close(self):
        if self._http:
            await self._http.aclose()
