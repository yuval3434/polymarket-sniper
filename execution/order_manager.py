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
    SIGNATURE_TYPE,
)
from config import TAKER_FEE_RATE
from market.state import MarketState

logger = logging.getLogger(__name__)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import (
        ApiCreds,
        MarketOrderArgs,
        OrderArgs,
        OrderType,
        PartialCreateOrderOptions,
    )
    from py_clob_client.constants import POLYGON
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
            # Mainnet: host=https://clob.polymarket.com  chain_id=137 (POLYGON)
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
            )
            logger.info(
                "ClobClient initialized — LIVE mode | host=%s chain_id=%s sig_type=%s funder=%s",
                POLY_HOST, POLYGON, SIGNATURE_TYPE, POLY_FUNDER_ADDRESS,
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
    ) -> dict | None:
        """
        Execute a snipe order.

        Maker path : GTC limit at ask_price - 0.001; waits _MAKER_WAIT_SECONDS;
                     falls back to market order if not filled.
        Taker path : immediate market order (FOK) via create_market_order.
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

        fee_bps = round(TAKER_FEE_RATE * 10_000)   # e.g. 0.018 → 180 bps

        if use_maker:
            maker_price = round(ask_price - 0.001, 4)
            order_args  = OrderArgs(
                token_id=token_id, price=maker_price, size=shares, side=side,
                fee_rate_bps=fee_bps,
            )
            options = PartialCreateOrderOptions()
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
                    await loop.run_in_executor(None, lambda: self._client.cancel(order_id))
                    result = await self._place_market_order(token_id, side, usdc, loop, fee_bps)
                else:
                    result = status_resp
                await self.verify_trade(order_id)
                return result
            except Exception as exc:
                logger.error("Maker order failed: %s — falling back to market order", exc)
                return await self._place_market_order(token_id, side, usdc, loop, fee_bps)
        else:
            return await self._place_market_order(token_id, side, usdc, loop, fee_bps)

    async def sell_with_retry(
        self,
        token_id: str,
        size_shares: float,
        bid_price: float,
    ) -> tuple[bool, float]:
        """
        Smart retry sell with exponential backoff and price aggression.

        Strategy:
          Attempt 1 : limit sell at bid_price          (current market)
          Attempt 2 : limit sell at bid_price − 0.02   (−2¢, become taker)
          Attempt 3 : limit sell at bid_price − 0.05   (−5¢, aggressive taker)
          Attempt 4 : limit sell at bid_price − 0.10   (−10¢, almost guaranteed fill)
          Attempt 5 : emergency limit at bid_price − 0.20  (any-price exit)

        After each order, the on-chain balance is queried to CONFIRM the position
        is gone. A 200 OK from the API is NOT treated as success.

        Returns: (success: bool, exit_price_used: float)
        """
        if self.dry_run or self._client is None:
            logger.info(
                "[PAPER] sell_with_retry: token=%s shares=%.4f bid=%.4f",
                token_id, size_shares, bid_price,
            )
            return True, bid_price

        MAX_RETRIES    = 5
        BACKOFF        = [0.5, 1.0, 2.0, 4.0, 8.0]
        PRICE_STEPS    = [0.00, 0.02, 0.05, 0.10, 0.20]   # cumulative discount per attempt
        MIN_FILL_PRICE = 0.01

        loop = asyncio.get_event_loop()

        # ── Step 1: resolve actual on-chain balance ────────────────────────
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

            async def _get_balance() -> float:
                resp = await loop.run_in_executor(
                    None,
                    lambda: self._client.get_balance_allowance(
                        params=BalanceAllowanceParams(
                            asset_type=AssetType.CONDITIONAL, token_id=token_id
                        )
                    ),
                )
                return int((resp or {}).get("balance", 0)) / 1e6

            balance = await _get_balance()
            logger.info(
                "SELL pre-check: on-chain balance=%.6f (theoretical=%.6f)",
                balance, size_shares,
            )
            if balance < 0.001:
                logger.info("Position already closed on-chain — no sell needed")
                return True, bid_price
            size_shares = balance   # always sell what we actually hold

        except Exception as exc:
            logger.warning("Balance pre-check failed — using theoretical size: %s", exc)
            _get_balance = None  # type: ignore[assignment]

        # ── Step 2: retry loop ─────────────────────────────────────────────
        prev_order_id: str = ""

        for attempt in range(1, MAX_RETRIES + 1):
            discount      = PRICE_STEPS[attempt - 1]
            current_price = max(MIN_FILL_PRICE, round(bid_price - discount, 4))

            logger.info(
                "SELL attempt %d/%d | token=%s | shares=%.4f | price=%.4f (discount=−%.2f)",
                attempt, MAX_RETRIES, token_id[:16], size_shares, current_price, discount,
            )

            # Cancel previous unfilled order before retrying at a lower price
            if prev_order_id:
                try:
                    await loop.run_in_executor(
                        None, lambda: self._client.cancel(prev_order_id)
                    )
                    logger.info("Cancelled unfilled order %s before retry", prev_order_id)
                except Exception as exc:
                    logger.warning("Could not cancel order %s: %s", prev_order_id, exc)
                prev_order_id = ""

            # Place the sell order
            try:
                order_args = OrderArgs(
                    token_id=token_id,
                    price=current_price,
                    size=size_shares,
                    side="SELL",
                )
                result = await loop.run_in_executor(
                    None,
                    lambda: self._client.create_and_post_order(
                        order_args, PartialCreateOrderOptions()
                    ),
                )
                _check_response(result, f"sell_attempt_{attempt}")
                prev_order_id = (result or {}).get("orderID") or (result or {}).get("id", "")
                logger.info(
                    "Sell order submitted (attempt %d): order_id=%s price=%.4f",
                    attempt, prev_order_id, current_price,
                )
            except Exception as exc:
                logger.warning("SELL attempt %d: API error placing order: %s", attempt, exc)

            # Wait for the order to (hopefully) fill
            delay = BACKOFF[attempt - 1]
            logger.info(
                "SELL attempt %d: waiting %.1fs then verifying on-chain balance…",
                attempt, delay,
            )
            await asyncio.sleep(delay)

            # ── Verify: query on-chain balance to confirm fill ─────────────
            try:
                remaining = await _get_balance()
                if remaining < 0.001:
                    logger.info(
                        "SELL CONFIRMED on attempt %d: balance=%.6f | exit_price=%.4f",
                        attempt, remaining, current_price,
                    )
                    return True, current_price
                else:
                    logger.warning(
                        "SELL attempt %d UNCONFIRMED: balance still %.6f shares — "
                        "order likely unfilled. Retrying with lower price.",
                        attempt, remaining,
                    )
            except Exception as exc:
                logger.warning(
                    "SELL attempt %d: balance verification failed: %s — "
                    "cannot confirm fill, retrying.",
                    attempt, exc,
                )

        # ── All retries exhausted ──────────────────────────────────────────
        logger.critical(
            "CRITICAL_ERROR: Failed to close position after %d attempts. "
            "token=%s size=%.4f initial_bid=%.4f. "
            "Manual intervention required!",
            MAX_RETRIES, token_id, size_shares, bid_price,
        )
        return False, bid_price

    async def emergency_sell(
        self,
        token_id: str,
        size_shares: float,
        bid_price: float,
    ) -> tuple[bool, float]:
        """
        Urgent sell triggered by adverse BTC momentum.

        Uses 2 attempts with minimal wait (200ms) and starts 3¢ below
        bid to cross the spread immediately. Accepts the haircut for speed.

        Returns: (success, exit_price_used)
        """
        if self.dry_run or self._client is None:
            logger.info(
                "[PAPER] emergency_sell: token=%s shares=%.4f bid=%.4f",
                token_id, size_shares, bid_price,
            )
            return True, bid_price

        STEPS       = [0.03, 0.12]   # aggressive from the first attempt
        WAIT        = 0.2            # 200ms — matches Binance latency budget
        MIN_PRICE   = 0.01

        loop = asyncio.get_event_loop()

        # Resolve actual on-chain balance
        async def _get_bal() -> float:
            try:
                from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                r = await loop.run_in_executor(
                    None,
                    lambda: self._client.get_balance_allowance(
                        params=BalanceAllowanceParams(
                            asset_type=AssetType.CONDITIONAL, token_id=token_id
                        )
                    ),
                )
                return int((r or {}).get("balance", 0)) / 1e6
            except Exception:
                return size_shares   # fallback to theoretical

        bal = await _get_bal()
        if bal < 0.001:
            logger.info("emergency_sell: position already closed on-chain")
            return True, bid_price
        size_shares = bal

        prev_order_id = ""
        for i, discount in enumerate(STEPS, 1):
            price = max(MIN_PRICE, round(bid_price - discount, 4))
            logger.warning(
                "EMERGENCY SELL attempt %d/%d: price=%.4f (−%.2f from bid=%.4f)",
                i, len(STEPS), price, discount, bid_price,
            )

            if prev_order_id:
                try:
                    await loop.run_in_executor(
                        None, lambda: self._client.cancel(prev_order_id)
                    )
                except Exception:
                    pass
                prev_order_id = ""

            try:
                result = await loop.run_in_executor(
                    None,
                    lambda: self._client.create_and_post_order(
                        OrderArgs(
                            token_id=token_id, price=price,
                            size=size_shares, side="SELL",
                        ),
                        PartialCreateOrderOptions(),
                    ),
                )
                _check_response(result, f"emergency_sell_{i}")
                prev_order_id = (result or {}).get("orderID") or (result or {}).get("id", "")
            except Exception as exc:
                logger.error("Emergency sell attempt %d: API error: %s", i, exc)

            await asyncio.sleep(WAIT)

            remaining = await _get_bal()
            if remaining < 0.001:
                logger.warning(
                    "EMERGENCY SELL confirmed on attempt %d: exit_price=%.4f", i, price,
                )
                return True, price

        logger.critical(
            "CRITICAL_ERROR: Emergency sell FAILED after %d attempts. "
            "token=%s size=%.4f — Manual intervention required!",
            len(STEPS), token_id, size_shares,
        )
        return False, bid_price

    async def try_redeem(
        self,
        condition_id: str,
        token_id: str,
        shares: float,
        entry_price: float,
    ) -> bool:
        """
        Auto-claim winning shares after market settlement.

        Only executes if estimated Polygon gas cost is less than
        max_gas_claim_pct (default 2%) of the gross profit.

        The py-clob-client exposes redeem_positions(condition_id).
        On Polygon PoS gas is typically < $0.002 per txn.
        """
        if self.dry_run or self._client is None:
            logger.info(
                "[PAPER] try_redeem: condition_id=%s shares=%.4f",
                condition_id, shares,
            )
            return False

        if shares < 0.001:
            logger.info("try_redeem: no shares to claim (shares=%.6f)", shares)
            return False

        import engine.strategy_loader as _scfg

        # Gross profit = (full $1 payout − what we paid) × shares held
        gross_profit_usd   = max(0.0, (1.0 - entry_price) * shares)
        max_gas_pct        = _scfg.max_gas_claim_pct()
        max_gas_usd        = gross_profit_usd * max_gas_pct

        # Conservative Polygon gas estimate (MATIC gas ~0.001–0.005 USD per txn)
        POLYGON_GAS_EST_USD = 0.005

        if gross_profit_usd <= 0:
            logger.warning("try_redeem: gross_profit=%.4f ≤ 0 — skipping claim", gross_profit_usd)
            return False

        if POLYGON_GAS_EST_USD > max_gas_usd:
            logger.warning(
                "Auto-claim skipped: est. gas $%.4f > allowed $%.4f "
                "(%.0f%% cap on profit $%.4f). condition_id=%s",
                POLYGON_GAS_EST_USD, max_gas_usd,
                max_gas_pct * 100, gross_profit_usd, condition_id,
            )
            return False

        logger.info(
            "Auto-claim: condition_id=%s shares=%.4f profit=$%.4f gas_est=$%.4f",
            condition_id, shares, gross_profit_usd, POLYGON_GAS_EST_USD,
        )
        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(
                None,
                lambda: self._client.redeem_positions(condition_id=condition_id),
            )
            logger.info("Auto-claim SUCCESS: condition_id=%s result=%s", condition_id, result)
            return True
        except Exception as exc:
            logger.error("Auto-claim FAILED: condition_id=%s error=%s", condition_id, exc)
            return False

    async def _place_market_order(
        self, token_id: str, side: str, amount_usdc: float, loop, fee_bps: int = 0
    ) -> dict | None:
        """Market order: create (sign) then post as FOK. Amount in USDC."""
        market_args = MarketOrderArgs(
            token_id=token_id,
            amount=amount_usdc,
            side=side,
            fee_rate_bps=fee_bps,
        )
        options = PartialCreateOrderOptions()
        try:
            signed_order = await loop.run_in_executor(
                None, lambda: self._client.create_market_order(market_args, options)
            )
            logger.info("Signed market order created: %s", signed_order)
            result = await loop.run_in_executor(
                None, lambda: self._client.post_order(signed_order, OrderType.FOK)
            )
            _check_response(result, "market")
            order_id = result.get("orderID") or result.get("id", "") if result else ""
            await self.verify_trade(order_id)
            return result
        except Exception as exc:
            logger.error("Market order failed: %s", exc)
            return None
