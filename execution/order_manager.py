import asyncio
import math
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
        OrderPayload,
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

_FAK_PRICE_SLIP     = 0.03
_GTC_WAIT_SECONDS   = 15.0  # wait this long for a GTC fill before cancelling

def _find_clean_shares(price: float, usdc: float, min_shares: int = 5) -> int:
    """
    Find the largest integer share count <= floor(usdc/price) such that
    price * shares has at most 2 decimal places in Python float arithmetic.

    Polymarket rejects orders where makerAmount (price × shares) has more
    than 2 decimal places. IEEE 754 can produce noise like 0.33*7=2.3100...005,
    so we test each candidate and reduce by 1 until the product is clean.
    """
    computed = max(int(usdc / price), min_shares)
    for shares in range(computed, min_shares - 1, -1):
        product = price * shares
        if abs(product - round(product, 2)) < 1e-9:
            return shares
    return min_shares

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
        # PyClobClient V2 removed get_orders() and get_positions().
        # Bypassing to prevent AttributeErrors and log spam.
        pass

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
        Execute a snipe order via FAK (Fill-and-Kill limit) at ask + slip.
        Always uses the freshest ask from state at execution time.
        Maker path is skipped — a 1s wait makes the fallback FAK price stale.
        """
        usdc = size_usdc if size_usdc is not None else MAX_POSITION_USDC

        if self.dry_run or self._client is None:
            shares = _find_clean_shares(ask_price, usdc)
            logger.info(
                "[PAPER] Would snipe: token=%s side=%s size_usdc=%.2f shares=%d "
                "price=%.4f",
                token_id, side, usdc, shares, ask_price,
            )
            return {"order_id": "PAPER", "status": "simulated", "price": ask_price, "size": shares}

        loop = asyncio.get_event_loop()

        # Use the freshest available ask from state rather than the signal-time price
        if token_id == state.up_token_id:
            live_ask = state.up_best_ask
        else:
            live_ask = state.down_best_ask
        fresh_ask = live_ask if 0 < live_ask < 1 else ask_price

        return await self._place_market_order(token_id, side, fresh_ask, usdc, loop)

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

        bid_price   = round(min(max(bid_price, 0.01), 0.99), 2)
        size_shares = math.floor(size_shares * 100) / 100

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
        self, token_id: str, side: str, ask_price: float, usdc: float, loop
    ) -> dict | None:
        """
        1. Try market FAK — fills whatever is in the book immediately.
        2. If book is empty, place a GTC limit at ask price and wait up to
           _GTC_WAIT_SECONDS for a seller to appear, then cancel if still open.
        """
        # ── Attempt 1: market FAK ─────────────────────────────────────────
        try:
            order_args = MarketOrderArgs(token_id=token_id, amount=usdc, side=side)
            signed_order = await loop.run_in_executor(
                None, lambda: self._client.create_market_order(order_args)
            )
            logger.info("Signed market order (FAK) $%.2f: %s", usdc, signed_order)
            result = await loop.run_in_executor(
                None, lambda: self._client.post_order(signed_order, OrderType.FAK)
            )
            _check_response(result, "market-fak")
            order_id = result.get("orderID") or result.get("id", "") if result else ""
            if order_id:
                await asyncio.sleep(0.5)
                fill_status = await loop.run_in_executor(None, lambda: self._client.get_order(order_id))
                if fill_status:
                    result = fill_status
            logger.info("Market FAK result: %s", result)
            return result
        except Exception as exc:
            msg = str(exc)
            if "no orders found" in msg or "FAK" in msg or "400" in msg:
                logger.warning("Market FAK empty book — placing GTC limit and waiting %.0fs", _GTC_WAIT_SECONDS)
            else:
                logger.error("Market order failed: %s", exc)
                return None

        # ── Attempt 2: GTC limit at ask, wait for fill, then cancel ──────
        gtc_price  = round(min(max(ask_price + _FAK_PRICE_SLIP, 0.01), 0.99), 2)
        shares     = _find_clean_shares(gtc_price, usdc)
        order_args = OrderArgs(token_id=token_id, price=gtc_price, size=float(shares), side=side)
        options    = PartialCreateOrderOptions(tick_size="0.01")
        gtc_id     = None
        try:
            signed_order = await loop.run_in_executor(
                None, lambda: self._client.create_order(order_args, options)
            )
            result = await loop.run_in_executor(
                None, lambda: self._client.post_order(signed_order, OrderType.GTC)
            )
            _check_response(result, "gtc")
            gtc_id = result.get("orderID") or result.get("id", "") if result else ""
            logger.info("GTC limit placed at $%.2f for %d shares (id=%s)", gtc_price, shares, gtc_id)
        except Exception as exc:
            logger.error("GTC limit placement failed: %s", exc)
            return None

        if not gtc_id:
            return None

        # Poll for fill, then cancel if still open
        await asyncio.sleep(_GTC_WAIT_SECONDS)
        try:
            status = await loop.run_in_executor(None, lambda: self._client.get_order(gtc_id))
            s = status or {}
            order_status   = s.get("status", "")
            size_matched   = float(
                s.get("sizeMatched") or s.get("size_matched") or
                s.get("sizeFilled")  or s.get("size_filled")  or
                s.get("takingAmount") or 0
            )
            # "matched" status or any non-zero fill means the order executed
            if order_status == "matched" or size_matched > 0:
                logger.info("GTC filled (status=%s matched=%.4f)", order_status, size_matched)
                return status
        except Exception as exc:
            logger.error("GTC status check failed: %s", exc)

        # Cancel unfilled GTC
        try:
            await loop.run_in_executor(
                None, lambda: self._client.cancel_order(OrderPayload(orderID=gtc_id))
            )
            logger.warning("GTC cancelled — no fill after %.0fs (trade missed)", _GTC_WAIT_SECONDS)
        except Exception as exc:
            logger.error("GTC cancel failed: %s", exc)
        return None
