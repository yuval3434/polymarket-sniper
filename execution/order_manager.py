import asyncio
import logging

from config import (
    DRY_RUN,
    MAX_POSITION_USDC,
    POLY_API_KEY,
    POLY_FUNDER_ADDRESS,
    POLY_HOST,
    POLY_PASSPHRASE,
    POLY_SECRET,
)
from market.state import MarketState

logger = logging.getLogger(__name__)

# Import lazily so the module can be imported even without py-clob-client installed
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
    from py_clob_client.constants import POLYGON
    _SDK_AVAILABLE = True
except ImportError:
    _SDK_AVAILABLE = False
    logger.warning("py-clob-client not installed — live execution disabled")

_MAKER_WAIT_SECONDS = 3.0   # time to wait for a maker fill before falling back to FAK
_FAK_PRICE_SLIP = 0.002     # cross the spread by this amount when using FAK


class OrderManager:
    def __init__(self) -> None:
        self._client = None
        if not DRY_RUN and _SDK_AVAILABLE:
            self._client = ClobClient(
                host=POLY_HOST,
                chain_id=POLYGON,
                creds=ApiCreds(
                    api_key=POLY_API_KEY,
                    api_secret=POLY_SECRET,
                    api_passphrase=POLY_PASSPHRASE,
                ),
                funder=POLY_FUNDER_ADDRESS,
            )
            logger.info("ClobClient initialized (LIVE mode)")
        elif DRY_RUN:
            logger.info("OrderManager in DRY_RUN mode — no real orders will be placed")
        else:
            logger.error("py-clob-client not available — cannot run in live mode")

    async def cancel_all(self) -> None:
        if self._client is None:
            return
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._client.cancel_all)
            logger.info("cancel_all() completed")
        except Exception as exc:
            logger.error("cancel_all() failed: %s", exc)

    async def snipe(
        self,
        token_id: str,
        side: str,          # "BUY" — we always buy the token we think wins
        ask_price: float,
        use_maker: bool,
        state: MarketState,
    ) -> dict | None:
        """
        Execute a snipe order.
        Maker path: place GTC limit at ask_price - 0.001, wait _MAKER_WAIT_SECONDS, then FAK.
        Taker path: FAK immediately at ask_price + _FAK_PRICE_SLIP.

        In DRY_RUN mode, returns a simulated fill dict without touching the API.
        """
        size_shares = MAX_POSITION_USDC / ask_price

        if DRY_RUN or self._client is None:
            logger.info(
                "[DRY RUN] Would snipe: token=%s side=%s size=%.4f price=%.4f maker=%s",
                token_id, side, size_shares, ask_price, use_maker,
            )
            return {
                "order_id": "DRY_RUN",
                "status": "simulated",
                "price": ask_price,
                "size": size_shares,
            }

        loop = asyncio.get_event_loop()

        if use_maker:
            maker_price = round(ask_price - 0.001, 4)
            order_args = OrderArgs(
                token_id=token_id,
                price=maker_price,
                size=size_shares,
                side=side,
            )
            try:
                result = await loop.run_in_executor(
                    None,
                    lambda: self._client.create_and_post_order(order_args),
                )
                logger.info("Maker order placed: %s", result)

                # Wait for fill; cancel and fall back to FAK if not filled
                await asyncio.sleep(_MAKER_WAIT_SECONDS)
                order_id = result.get("orderID") or result.get("id", "")
                order_status = await loop.run_in_executor(
                    None, lambda: self._client.get_order(order_id)
                )
                status = (order_status or {}).get("status", "")
                if status not in ("MATCHED", "FILLED"):
                    logger.info(
                        "Maker not filled (%s) — cancelling and falling back to FAK", status
                    )
                    await loop.run_in_executor(
                        None, lambda: self._client.cancel(order_id)
                    )
                    return await self._place_fak(token_id, side, ask_price, size_shares, loop)
                return order_status
            except Exception as exc:
                logger.error("Maker order failed: %s — falling back to FAK", exc)
                return await self._place_fak(token_id, side, ask_price, size_shares, loop)
        else:
            return await self._place_fak(token_id, side, ask_price, size_shares, loop)

    async def _place_fak(
        self,
        token_id: str,
        side: str,
        ask_price: float,
        size_shares: float,
        loop,
    ) -> dict | None:
        fak_price = round(min(ask_price + _FAK_PRICE_SLIP, 0.999), 4)
        order_args = OrderArgs(
            token_id=token_id,
            price=fak_price,
            size=size_shares,
            side=side,
        )
        try:
            result = await loop.run_in_executor(
                None,
                lambda: self._client.create_and_post_order(order_args, OrderType.FAK),
            )
            logger.info("FAK order placed: %s", result)
            return result
        except Exception as exc:
            logger.error("FAK order failed: %s", exc)
            return None
