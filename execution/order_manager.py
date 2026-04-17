import asyncio
import logging

from config import (
    ETH_PRIVATE_KEY,
    MAX_POSITION_USDC,
    POLY_API_KEY,
    POLY_FUNDER_ADDRESS,
    POLY_HOST,
    POLY_PASSPHRASE,
    POLY_SECRET,
)
from market.state import MarketState

logger = logging.getLogger(__name__)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import (
        ApiCreds,
        MarketOrderArgs,
        OrderArgs,
        PartialCreateOrderOptions,
    )
    from py_clob_client.constants import POLYGON
    _SDK_AVAILABLE = True
except ImportError:
    _SDK_AVAILABLE = False
    logger.warning("py-clob-client not installed — live execution disabled")

_MAKER_WAIT_SECONDS = 3.0


class OrderManager:
    def __init__(self, dry_run: bool = True) -> None:
        self.dry_run = dry_run
        self._client = None

        if not dry_run and _SDK_AVAILABLE:
            self._client = ClobClient(
                host=POLY_HOST,
                key=ETH_PRIVATE_KEY,
                chain_id=POLYGON,
                creds=ApiCreds(
                    api_key=POLY_API_KEY,
                    api_secret=POLY_SECRET,
                    api_passphrase=POLY_PASSPHRASE,
                ),
                funder=POLY_FUNDER_ADDRESS,
            )
            logger.info("ClobClient initialized — LIVE mode")
        elif dry_run:
            logger.info("OrderManager in paper mode — no real orders will be placed")
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
        side: str,
        ask_price: float,
        use_maker: bool,
        state: MarketState,
        size_usdc: float | None = None,
    ) -> dict | None:
        """
        Execute a snipe order.

        Maker path : GTC limit at ask_price - 0.001; waits _MAKER_WAIT_SECONDS;
                     falls back to market order if not filled.
        Taker path : immediate market order (FOK) via create_market_order.

        size_usdc  : USDC to spend (after live guardrails). Falls back to
                     MAX_POSITION_USDC if not provided.
        """
        usdc   = size_usdc if size_usdc is not None else MAX_POSITION_USDC
        shares = usdc / ask_price

        if self.dry_run or self._client is None:
            logger.info(
                "[PAPER] Would snipe: token=%s side=%s size_usdc=%.2f shares=%.4f "
                "price=%.4f maker=%s",
                token_id, side, usdc, shares, ask_price, use_maker,
            )
            return {"order_id": "PAPER", "status": "simulated", "price": ask_price, "size": shares}

        loop = asyncio.get_event_loop()

        if use_maker:
            maker_price = round(ask_price - 0.001, 4)
            order_args  = OrderArgs(
                token_id=token_id, price=maker_price, size=shares, side=side
            )
            # PartialCreateOrderOptions() lets the SDK auto-fetch tick_size / neg_risk
            options = PartialCreateOrderOptions()
            try:
                result = await loop.run_in_executor(
                    None, lambda: self._client.create_and_post_order(order_args, options)
                )
                logger.info("Maker order placed: %s", result)
                await asyncio.sleep(_MAKER_WAIT_SECONDS)
                order_id    = result.get("orderID") or result.get("id", "")
                status_resp = await loop.run_in_executor(
                    None, lambda: self._client.get_order(order_id)
                )
                if (status_resp or {}).get("status") not in ("MATCHED", "FILLED"):
                    logger.info("Maker not filled — cancelling, falling back to market order")
                    await loop.run_in_executor(None, lambda: self._client.cancel(order_id))
                    return await self._place_market_order(token_id, side, usdc, loop)
                return status_resp
            except Exception as exc:
                logger.error("Maker order failed: %s — falling back to market order", exc)
                return await self._place_market_order(token_id, side, usdc, loop)
        else:
            return await self._place_market_order(token_id, side, usdc, loop)

    async def _place_market_order(
        self, token_id: str, side: str, amount_usdc: float, loop
    ) -> dict | None:
        """
        Market order via create_market_order (FOK — Fill or Kill).
        Uses amount in USDC, not shares.
        """
        market_args = MarketOrderArgs(
            token_id=token_id,
            amount=amount_usdc,
            side=side,
        )
        options = PartialCreateOrderOptions()
        try:
            result = await loop.run_in_executor(
                None, lambda: self._client.create_market_order(market_args, options)
            )
            logger.info("Market order placed: %s", result)
            return result
        except Exception as exc:
            logger.error("Market order failed: %s", exc)
            return None
