import asyncio
import logging

from config import (
    ETH_PRIVATE_KEY,
    MAX_POSITION_USDC,
    POLY_API_KEY,
    POLY_BUILDER_CODE,
    POLY_FUNDER_ADDRESS,
    POLY_HOST,
    POLY_PASSPHRASE,
    POLY_SECRET,
    SIGNATURE_TYPE,
)
from market.state import MarketState

logger = logging.getLogger(__name__)

try:
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import (
        ApiCreds,
        BuilderConfig,
        MarketOrderArgs,
        OrderArgs,
        OrderType,
        PartialCreateOrderOptions,
    )
    from py_clob_client_v2.constants import POLYGON
    _SDK_AVAILABLE = True
except ImportError:
    _SDK_AVAILABLE = False
    logger.warning("py-clob-client not installed — live execution disabled")

_MAKER_WAIT_SECONDS = 3.0


def _check_response(result: dict | None, label: str) -> None:
    """Log raw response and raise on silent failure."""
    logger.info("RAW API Response [%s]: %s", label, result)
    if result is None:
        logger.critical("SILENT FAILURE [%s]: response is None", label)
        return
    if isinstance(result, dict):
        if result.get("success") is False or result.get("errorMsg") or result.get("error"):
            logger.critical(
                "ORDER REJECTED [%s]: success=%s error=%s errorMsg=%s",
                label,
                result.get("success"),
                result.get("error"),
                result.get("errorMsg"),
            )


class OrderManager:
    def __init__(self, dry_run: bool = True) -> None:
        self.dry_run = dry_run
        self._client = None

        if not dry_run and _SDK_AVAILABLE:
            builder_cfg = BuilderConfig(builder_code=POLY_BUILDER_CODE) if POLY_BUILDER_CODE else None
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
                signature_type=SIGNATURE_TYPE,
                builder_config=builder_cfg,
            )
            logger.info(
                "ClobClient initialized — LIVE mode | host=%s chain_id=%s sig_type=%s funder=%s builder=%s",
                POLY_HOST, POLYGON, SIGNATURE_TYPE, POLY_FUNDER_ADDRESS,
                "set" if builder_cfg else "unset",
            )
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

    async def verify_trade(self, order_id: str) -> None:
        """Query open orders and positions immediately after a trade to confirm API visibility."""
        if self._client is None:
            return
        loop = asyncio.get_event_loop()
        try:
            open_orders = await loop.run_in_executor(None, self._client.get_orders)
            logger.info("POST-TRADE open orders: %s", open_orders)
        except Exception as exc:
            logger.warning("get_orders() failed: %s", exc)
        try:
            positions = await loop.run_in_executor(None, self._client.get_positions)
            logger.info("POST-TRADE positions: %s", positions)
        except Exception as exc:
            logger.warning("get_positions() failed: %s", exc)

    async def snipe(
        self,
        token_id: str,
        side: str,
        ask_price: float,
        use_maker: bool,
        state: MarketState,
        size_usdc: float | None = None,
        p_true: float = 0.5,
        user_usdc_balance: float = 0,
    ) -> dict | None:
        """
        Execute a snipe order.

        Maker path : GTC limit at ask_price - 0.001; waits _MAKER_WAIT_SECONDS;
                     falls back to market order if not filled.
        Taker path : immediate market order (FOK) via create_market_order.
        """
        usdc   = size_usdc if size_usdc is not None else MAX_POSITION_USDC
        # Round strictly to 2 decimal places. The backend snaps values to tick sizes (usually 0.01).
        # Signing un-snapped floats causes a hash mismatch and "invalid signature".
        shares = round(usdc / ask_price, 2)

        if self.dry_run or self._client is None:
            logger.info(
                "[PAPER] Would snipe: token=%s side=%s size_usdc=%.2f shares=%.4f "
                "price=%.4f maker=%s",
                token_id, side, usdc, shares, ask_price, use_maker,
            )
            return {"order_id": "PAPER", "status": "simulated", "price": ask_price, "size": shares}

        loop = asyncio.get_event_loop()

        if use_maker:
            maker_price = round(max(ask_price - 0.01, 0.01), 2)
            order_args  = OrderArgs(
                token_id=token_id, price=maker_price, size=shares, side=side,
            )
            options = PartialCreateOrderOptions(tick_size="0.01")
            try:
                result = await loop.run_in_executor(
                    None, lambda: self._client.create_and_post_order(order_args, options)
                )
                _check_response(result, "maker")
                await asyncio.sleep(_MAKER_WAIT_SECONDS)
                order_id    = result.get("orderID") or result.get("id", "") if result else ""
                status_resp = await loop.run_in_executor(
                    None, lambda: self._client.get_order(order_id)
                )
                logger.info("Maker order status: %s", status_resp)
                if (status_resp or {}).get("status") not in ("MATCHED", "FILLED"):
                    logger.info("Maker not filled — cancelling, falling back to market order")
                    await loop.run_in_executor(None, lambda: self._client.cancel_order(order_id))
                    result = await self._place_market_order(token_id, side, usdc, loop, user_usdc_balance)
                else:
                    result = status_resp
                await self.verify_trade(order_id)
                return result
            except Exception as exc:
                logger.error("Maker order failed: %s — falling back to market order", exc)
                return await self._place_market_order(token_id, side, usdc, loop, user_usdc_balance)
        else:
            return await self._place_market_order(token_id, side, usdc, loop, user_usdc_balance)

    async def sell(
        self,
        token_id: str,
        size_shares: float,
        bid_price: float,
    ) -> dict | None:
        """Sell (close) a position by placing a limit SELL at bid_price.
        Queries the actual on-chain token balance first to avoid over-selling.
        """
        if self.dry_run or self._client is None:
            logger.info("[PAPER] Would sell: token=%s shares=%.4f bid=%.4f", token_id, size_shares, bid_price)
            return {"status": "simulated_sell"}

        loop = asyncio.get_event_loop()

        # Query actual on-chain balance — may differ from theoretical due to partial fills / fees
        try:
            from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
            bal_resp = await loop.run_in_executor(
                None,
                lambda: self._client.get_balance_allowance(
                    params=BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
                ),
            )
            actual_shares = int(bal_resp.get("balance", 0)) / 1e6
            logger.info(
                "On-chain token balance: %.6f shares (theoretical: %.6f)",
                actual_shares, size_shares,
            )
            if actual_shares < 0.001:
                logger.warning("On-chain balance near zero (%.6f) — position may already be closed", actual_shares)
                return {"status": "already_closed"}
            size_shares = actual_shares
        except Exception as exc:
            logger.warning("Could not fetch token balance, using theoretical size: %s", exc)

        bid_price = round(min(max(bid_price, 0.01), 0.99), 2)
        size_shares = round(size_shares, 2)

        order_args = OrderArgs(
            token_id=token_id,
            price=bid_price,
            size=size_shares,
            side="SELL",
        )
        options = PartialCreateOrderOptions(tick_size="0.01")
        try:
            result = await loop.run_in_executor(
                None, lambda: self._client.create_and_post_order(order_args, options)
            )
            _check_response(result, "sell")
            logger.info("Sell order placed: %s", result)
            return result
        except Exception as exc:
            logger.error("Sell order failed: %s", exc)
            return None

    async def _place_market_order(
        self, token_id: str, side: str, amount_usdc: float, loop, user_usdc_balance: float = 0
    ) -> dict | None:
        """Market order: create (sign) then post as FAK. Amount in USDC."""
        market_args = MarketOrderArgs(
            token_id=token_id,
            amount=amount_usdc,
            side=side,
            user_usdc_balance=user_usdc_balance,
        )
        options = PartialCreateOrderOptions()
        try:
            signed_order = await loop.run_in_executor(
                None, lambda: self._client.create_market_order(market_args, options)
            )
            logger.info("Signed market order created: %s", signed_order)
            result = await loop.run_in_executor(
                None, lambda: self._client.post_order(signed_order, OrderType.FAK)
            )
            _check_response(result, "market")
            order_id = result.get("orderID") or result.get("id", "") if result else ""
            await self.verify_trade(order_id)
            return result
        except Exception as exc:
            logger.error("Market order failed: %s", exc)
            return None
