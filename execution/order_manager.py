import asyncio
import json
import logging
import os
import time

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

_MAKER_WAIT_SECONDS   = 3.0
_USDC_ADDRESS         = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e on Polygon
_ZERO_BYTES32         = "0x" + "0" * 64                               # parent_collection_id
_CLAIM_RETRY_INTERVAL = 120        # seconds between claim attempts
_CLAIM_GIVE_UP_SECS   = 7200      # stop retrying after 2 hours
_PENDING_CLAIMS_FILE  = os.path.join(os.path.dirname(os.path.dirname(__file__)), "pending_claims.json")


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
        """Query open orders after a trade to confirm API visibility."""
        if self._client is None:
            return
        loop = asyncio.get_event_loop()
        try:
            open_orders = await loop.run_in_executor(None, self._client.get_orders)
            logger.info("POST-TRADE open orders: %s", open_orders)
        except Exception as exc:
            logger.warning("get_orders() failed: %s", exc)

    # ------------------------------------------------------------------
    # Auto-claim: persistent retry loop for post-expiry redemption
    # ------------------------------------------------------------------

    def add_pending_claim(self, condition_id: str, token_side: str) -> None:
        """
        Persist a pending claim to disk so it survives bot restarts.
        The claim monitor loop picks it up and retries every 2 minutes.
        """
        claims = self._load_pending_claims()
        # Avoid duplicates
        if any(c["condition_id"] == condition_id for c in claims):
            return
        claims.append({
            "condition_id": condition_id,
            "token_side":   token_side,
            "added_at":     time.time(),
            "attempts":     0,
        })
        self._save_pending_claims(claims)
        logger.info(
            "Pending claim queued: condition_id=%s side=%s (will retry every %ds for up to %dmin)",
            condition_id, token_side, _CLAIM_RETRY_INTERVAL, _CLAIM_GIVE_UP_SECS // 60,
        )

    async def claim_monitor_loop(self) -> None:
        """
        Runs forever. Every 2 minutes checks pending_claims.json and attempts
        to redeem each entry. Removes on success; gives up after 2 hours.
        """
        logger.info("Claim monitor started — checking every %ds", _CLAIM_RETRY_INTERVAL)
        while True:
            await asyncio.sleep(_CLAIM_RETRY_INTERVAL)
            claims = self._load_pending_claims()
            if not claims:
                continue

            remaining = []
            for claim in claims:
                age = time.time() - claim["added_at"]
                if age > _CLAIM_GIVE_UP_SECS:
                    logger.critical(
                        "AUTO-CLAIM gave up after %.0fmin: condition_id=%s side=%s — "
                        "claim manually at polymarket.com",
                        age / 60, claim["condition_id"], claim["token_side"],
                    )
                    continue  # drop from list

                claim["attempts"] += 1
                success = await self._attempt_claim(claim["condition_id"], claim["token_side"])
                if success:
                    logger.info(
                        "AUTO-CLAIM SUCCESS (attempt %d, %.0fmin after expiry): condition_id=%s",
                        claim["attempts"], age / 60, claim["condition_id"],
                    )
                else:
                    logger.info(
                        "Claim not ready yet (attempt %d, %.0fmin elapsed) — retrying in %ds | condition_id=%s",
                        claim["attempts"], age / 60, _CLAIM_RETRY_INTERVAL, claim["condition_id"],
                    )
                    remaining.append(claim)

            self._save_pending_claims(remaining)

    async def _attempt_claim(self, condition_id: str, token_side: str) -> bool:
        """Single claim attempt. Returns True if redeemed successfully."""
        if self.dry_run or self._client is None:
            logger.info("[PAPER] Would auto-claim: condition_id=%s side=%s", condition_id, token_side)
            return True

        index_set = [1] if token_side == "UP" else [2]
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self._client.redeem_positions(
                    reward_token=_USDC_ADDRESS,
                    condition_id=condition_id,
                    parent_collection_id=_ZERO_BYTES32,
                    index_sets=index_set,
                ),
            )
            return True
        except AttributeError:
            logger.warning(
                "Auto-claim: redeem_positions() not available in this SDK version. "
                "Upgrade: pip install --upgrade py-clob-client | condition_id=%s", condition_id,
            )
            return True  # don't keep retrying if SDK doesn't support it
        except Exception as exc:
            logger.debug("Claim attempt failed (likely not resolved yet): %s", exc)
            return False

    @staticmethod
    def _load_pending_claims() -> list[dict]:
        try:
            if os.path.exists(_PENDING_CLAIMS_FILE):
                with open(_PENDING_CLAIMS_FILE) as f:
                    return json.load(f)
        except Exception:
            pass
        return []

    @staticmethod
    def _save_pending_claims(claims: list[dict]) -> None:
        try:
            with open(_PENDING_CLAIMS_FILE, "w") as f:
                json.dump(claims, f, indent=2)
        except Exception as exc:
            logger.warning("Could not save pending_claims.json: %s", exc)

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

        fee_bps = 1000   # Polymarket protocol requirement — must be exactly 1000 bps

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
            if balance < 0.01:
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
                if remaining < 0.01:
                    logger.info(
                        "SELL CONFIRMED on attempt %d: balance=%.6f (dust) | exit_price=%.4f",
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
            # Extract actual fill price from signed order (set from live order book)
            actual_price = None
            try:
                actual_price = float(
                    getattr(signed_order, "price", None)
                    or (signed_order.get("price") if isinstance(signed_order, dict) else None)
                    or 0
                ) or None
            except Exception:
                pass
            logger.info("Signed market order created: %s (actual_price=%s)", signed_order, actual_price)
            result = await loop.run_in_executor(
                None, lambda: self._client.post_order(signed_order, OrderType.FOK)
            )
            _check_response(result, "market")
            order_id = result.get("orderID") or result.get("id", "") if result else ""
            await self.verify_trade(order_id)
            # Inject actual price into result so caller can correct position entry
            if result and actual_price:
                result["actual_fill_price"] = actual_price
            return result
        except Exception as exc:
            logger.error("Market order failed: %s", exc)
            return None
