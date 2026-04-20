import asyncio
import json
import logging
import os
import time

import aiohttp

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

# ── Entry execution guardrails ─────────────────────────────────────────────
_MAX_SLIPPAGE_CENTS  = 0.03   # IOC limit price = signal_ask + 3¢ — never sweep beyond this
_MIN_LIQUIDITY_USDC  = 5.0    # reject trade if book depth < $5 within slippage band
_MIN_FILL_USDC       = 3.0    # warn as tiny fill if IOC settles < $3

# ── Exit execution guardrails (per exit type) ──────────────────────────────
_EXIT_SLIPPAGE_STOP   = 0.05  # STOP_LOSS: accept up to 5¢ below bid (speed > price)
_EXIT_SLIPPAGE_TRAIL  = 0.03  # TRAIL_BREAKEVEN: balanced — 3¢ below bid
_EXIT_SLIPPAGE_PROFIT = 0.02  # TRAIL_PROFIT_FLOOR: price quality — 2¢ below bid
_MAKER_TRY_SECONDS    = 2.0   # wait for passive maker sell before falling back to IOC
_CHUNK_WAIT_SECONDS   = 3.0   # pause between partial exit chunks (book replenishment)
_CHUNK_THIN_RATIO     = 0.80  # chunk if available bid depth < 80% of position size

# ── API rate limiter (leaky bucket) ────────────────────────────────────────────
_RATE_LIMIT_CALLS_PER_SEC = 6   # max SDK calls/s — well below Polymarket's ~10/s limit


class _RateLimiter:
    """
    Token-bucket rate limiter for py-clob-client SDK calls.

    Enforces at most `rate` calls per second.  Callers await acquire() before
    every live SDK call; the coroutine sleeps only when the bucket is empty.
    Thread-safe via asyncio — no locking needed in a single-event-loop design.
    """

    def __init__(self, rate: float = _RATE_LIMIT_CALLS_PER_SEC) -> None:
        self._rate        = rate
        self._tokens      = rate          # start full
        self._last_refill = time.monotonic()

    async def acquire(self) -> None:
        """Wait until a token is available, then consume one."""
        while True:
            now     = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens      = min(self._rate, self._tokens + elapsed * self._rate)
            self._last_refill = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return

            wait = (1.0 - self._tokens) / self._rate
            logger.debug("RateLimiter: bucket empty — sleeping %.3fs", wait)
            await asyncio.sleep(wait)


async def _fetch_ask_depth_usdc(token_id: str, max_price: float) -> tuple[float, float]:
    """
    Query Polymarket CLOB /book and return (usdc_available, shares_available)
    across all ask levels priced <= max_price. Returns (0, 0) on any error.
    """
    url = f"{POLY_HOST}/book"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, params={"token_id": token_id},
                timeout=aiohttp.ClientTimeout(total=3),
            ) as resp:
                if resp.status != 200:
                    return 0.0, 0.0
                data = await resp.json()
        usdc_sum  = 0.0
        share_sum = 0.0
        for level in data.get("asks", []):
            p = float(level.get("price", 999))
            s = float(level.get("size", 0))
            if p <= max_price:
                usdc_sum  += p * s
                share_sum += s
        return usdc_sum, share_sum
    except Exception as exc:
        logger.warning("Book depth fetch failed (skipping liquidity check): %s", exc)
        return 0.0, 0.0


async def _fetch_bid_depth_shares(token_id: str, min_price: float) -> tuple[float, float]:
    """
    Query Polymarket CLOB /book and return (shares_available, usdc_available)
    across all bid levels priced >= min_price. Returns (0, 0) on any error.
    """
    url = f"{POLY_HOST}/book"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, params={"token_id": token_id},
                timeout=aiohttp.ClientTimeout(total=3),
            ) as resp:
                if resp.status != 200:
                    return 0.0, 0.0
                data = await resp.json()
        share_sum = 0.0
        usdc_sum  = 0.0
        for level in data.get("bids", []):
            p = float(level.get("price", 0))
            s = float(level.get("size", 0))
            if p >= min_price:
                share_sum += s
                usdc_sum  += p * s
        return share_sum, usdc_sum
    except Exception as exc:
        logger.warning("Bid depth fetch failed (proceeding without liquidity check): %s", exc)
        return 0.0, 0.0


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

        # Rate limiter shared across all SDK call sites — applies in live mode only
        self._rate_limiter = _RateLimiter(_RATE_LIMIT_CALLS_PER_SEC)


    async def _sdk_call(self, func):
        """Execute a py-clob-client lambda function with automatic retry for 425 'service not ready'."""
        loop = __import__('asyncio').get_event_loop()
        max_attempts = 30
        for attempt in range(1, max_attempts + 1):
            await self._rate_limiter.acquire()
            try:
                return await loop.run_in_executor(None, func)
            except Exception as exc:
                exc_str = str(exc).lower()
                if "425" in exc_str or "service not ready" in exc_str:
                    if attempt < max_attempts:
                        delay = min(1.0, 0.25 * (2 ** (attempt - 1)))
                        import logging
                        logging.getLogger(__name__).warning("API 425 'service not ready' — waiting %.2fs and retrying (attempt %d/%d)", delay, attempt, max_attempts)
                        await __import__('asyncio').sleep(delay)
                        continue
                raise

    async def cancel_all(self) -> None:
        if self._client is None:
            return
        try:
            loop = asyncio.get_event_loop()
            await self._rate_limiter.acquire()
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
            await self._rate_limiter.acquire()
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

    @staticmethod
    def _safe_order_amounts(amount_usdc: float, limit_price: float) -> tuple[float, float]:
        """
        Returns (size_shares, actual_usdc) satisfying Polymarket's hard precision rules:

          takerAmount (6-decimal int) = round(shares × 1e6)  →  must be divisible by 100
                                                                  (= shares have ≤ 4 dp)
          makerAmount (6-decimal int) = round(price × takerAmount) → must be divisible by 10000
                                                                      (= USDC has ≤ 2 dp)

        Simple rounding fails because the SDK recomputes makerAmount from price × size
        internally in float — we cannot control that product directly.  Instead we use
        integer GCD arithmetic to find the LARGEST share count, expressed in 0.0001-tick
        units, that makes price × shares an exact multiple of 0.01.

        Returns (0.0, 0.0) if no valid quantity fits within budget.
        """
        from math import gcd as _gcd
        # Work entirely in integer ticks to avoid IEEE-754 drift.
        # price_4dp : price in 0.0001 units  (e.g. 6160 for $0.6160)
        # share_unit: shares in 0.0001 units (e.g. 206250 → 20.6250 shares)
        price_4dp   = round(limit_price * 10_000)
        budget_cent = round(amount_usdc * 100)          # USDC budget in integer cents

        if price_4dp <= 0 or budget_cent <= 0:
            return 0.0, 0.0

        # ── Find minimum valid share granularity ──────────────────────────────
        # makerAmount_6dp = (price_4dp / 10_000) × (share_units × 100)
        #                 = price_4dp × share_units / 100
        # For divisibility by 10_000:
        #   price_4dp × share_units % 1_000_000 == 0
        #   → share_units must be a multiple of  1_000_000 // gcd(price_4dp, 1_000_000)
        g          = _gcd(price_4dp, 1_000_000)
        share_step = 1_000_000 // g             # e.g. 10000 for price=0.63 → whole shares only

        # ── Max share_units that fits within USDC budget ──────────────────────
        # shares = budget_usdc / price
        # share_units = shares × 10_000 = (budget_cent/100) / (price_4dp/10_000) × 10_000
        #             = budget_cent × 1_000_000 // price_4dp   (integer floor)
        max_units   = budget_cent * 1_000_000 // price_4dp
        valid_units = (max_units // share_step) * share_step    # floor to nearest valid step

        if valid_units <= 0:
            return 0.0, 0.0

        size_shares = valid_units / 10_000
        # Exact USDC cost — price_4dp × valid_units is guaranteed divisible by 1_000_000
        # USDC_cents = price_4dp × valid_units / 1_000_000  (exact integer)
        usdc_cents  = price_4dp * valid_units // 1_000_000
        actual_usdc = usdc_cents / 100          # e.g. 1260 → $12.60
        return size_shares, actual_usdc

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

        # ── Slippage cap ──────────────────────────────────────────────────────
        max_acceptable_price = round(ask_price + _MAX_SLIPPAGE_CENTS, 4)

        # ── Liquidity pre-check ───────────────────────────────────────────────
        avail_usdc, avail_shares = await _fetch_ask_depth_usdc(token_id, max_acceptable_price)
        logger.info(
            "Liquidity check: signal=$%.4f cap=$%.4f | "
            "book_depth=$%.2f (%.2f shares) | order=$%.2f",
            ask_price, max_acceptable_price, avail_usdc, avail_shares, usdc,
        )
        if avail_usdc < _MIN_LIQUIDITY_USDC:
            logger.warning(
                "LIQUIDITY REJECTED: only $%.2f available within %.0f¢ slippage "
                "(need >$%.2f) — trade skipped to avoid book-sweep slippage",
                avail_usdc, _MAX_SLIPPAGE_CENTS * 100, _MIN_LIQUIDITY_USDC,
            )
            return None

        # Cap bet to available depth so we don't partial-fill into worse levels
        if avail_usdc < usdc:
            logger.warning(
                "ORDER CAPPED by liquidity: $%.2f → $%.2f "
                "(%.2f shares within %.0f¢ of ask)",
                usdc, avail_usdc, avail_shares, _MAX_SLIPPAGE_CENTS * 100,
            )
            usdc = avail_usdc   # cap budget; do NOT use avail_shares directly (precision unsafe)

        if use_maker:
            maker_price = round(ask_price - 0.001, 4)
            # ── Precision-safe maker amounts (GCD arithmetic) ─────────────────
            m_shares, m_usdc = self._safe_order_amounts(usdc, maker_price)
            if m_shares <= 0:
                logger.warning(
                    "Maker path: no valid share qty for price=%.4f budget=$%.2f — "
                    "falling back to IOC", maker_price, usdc,
                )
                return await self._place_ioc_limit_order(
                    token_id, side, usdc, ask_price, max_acceptable_price, loop, fee_bps
                )
            order_args = OrderArgs(
                token_id=token_id, price=maker_price, size=m_shares, side=side,
                fee_rate_bps=fee_bps,
            )
            options = PartialCreateOrderOptions()
            try:
                result = await self._sdk_call(
                    lambda: self._client.create_and_post_order(order_args, options)
                )
                _check_response(result, "maker")
                await asyncio.sleep(_MAKER_WAIT_SECONDS)
                order_id    = result.get("orderID") or result.get("id", "") if result else ""
                await self._rate_limiter.acquire()
                status_resp = await loop.run_in_executor(
                    None, lambda: self._client.get_order(order_id)
                )
                logger.info("Maker order status: %s", status_resp)
                if (status_resp or {}).get("status") not in ("MATCHED", "FILLED"):
                    logger.info("Maker not filled — cancelling, falling back to IOC limit")
                    await self._rate_limiter.acquire()
                    await loop.run_in_executor(None, lambda: self._client.cancel(order_id))
                    return await self._place_ioc_limit_order(
                        token_id, side, usdc, ask_price, max_acceptable_price, loop, fee_bps
                    )
                # Maker filled — inject verified fill data
                if status_resp:
                    status_resp["actual_fill_price"]  = maker_price
                    status_resp["actual_fill_shares"] = m_shares
                    status_resp["actual_fill_usdc"]   = m_usdc
                await self.verify_trade(order_id)
                return status_resp
            except Exception as exc:
                logger.error("Maker order failed: %s — falling back to IOC limit", exc)
                return await self._place_ioc_limit_order(
                    token_id, side, usdc, ask_price, max_acceptable_price, loop, fee_bps
                )
        else:
            return await self._place_ioc_limit_order(
                token_id, side, usdc, ask_price, max_acceptable_price, loop, fee_bps
            )

    # ------------------------------------------------------------------
    # Smart exit — replaces sell_with_retry for live positions
    # ------------------------------------------------------------------

    async def smart_exit(
        self,
        token_id: str,
        size_shares: float,
        bid_price: float,
        exit_type: str,
    ) -> tuple[bool, float]:
        """
        Liquidity-aware exit with per-type slippage tolerance.

        Exit type routing:
          STOP_LOSS          → 5¢ floor, IOC immediately, no chunking (speed)
          TRAIL_BREAKEVEN    → 3¢ floor, IOC, chunked if book thin
          TRAIL_PROFIT_FLOOR → 2¢ floor, passive maker attempt first, chunked

        Returns (success, avg_exit_price).
        """
        if self.dry_run or self._client is None:
            logger.info(
                "[PAPER] smart_exit [%s]: token=%s shares=%.4f bid=%.4f",
                exit_type, token_id[:16], size_shares, bid_price,
            )
            return True, bid_price

        loop = asyncio.get_event_loop()

        # ── Routing by exit type ─────────────────────────────────────────
        if exit_type == "STOP_LOSS":
            max_slippage  = _EXIT_SLIPPAGE_STOP
            maker_first   = False
            allow_chunks  = False   # speed critical — don't wait for replenishment
        elif exit_type == "TRAIL_PROFIT_FLOOR":
            max_slippage  = _EXIT_SLIPPAGE_PROFIT
            maker_first   = True    # try passive limit at bid first
            allow_chunks  = True
        else:  # TRAIL_BREAKEVEN and any other
            max_slippage  = _EXIT_SLIPPAGE_TRAIL
            maker_first   = False
            allow_chunks  = True

        floor_price = max(0.01, round(bid_price - max_slippage, 4))

        # ── Verify actual on-chain balance ───────────────────────────────
        balance = await self._get_token_balance(token_id, loop)
        logger.info(
            "EXIT [%s]: on-chain balance=%.4f (theoretical=%.4f) | "
            "bid=%.4f floor=%.4f",
            exit_type, balance, size_shares, bid_price, floor_price,
        )
        # Dust guard: raise threshold to 0.01 shares (min viable sell order).
        # A balance of 0.001–0.009 shares can slip through and produce a
        # taker_amount that rounds to 0 inside the SDK's int() conversion.
        if balance < 0.01:
            logger.info(
                "Position closed (on-chain balance %.6f < 0.01 dust threshold) — no sell needed",
                balance,
            )
            return True, bid_price
        size_shares = round(balance, 4)  # canonicalise to 4dp before any math

        # ── Bid depth pre-check ──────────────────────────────────────────
        avail_shares, avail_usdc = await _fetch_bid_depth_shares(token_id, floor_price)
        logger.info(
            "Bid depth: %.4f shares ($%.2f) available at bids >= $%.4f | need %.4f shares",
            avail_shares, avail_usdc, floor_price, size_shares,
        )

        # ── Maker-first for profitable exits ─────────────────────────────
        if maker_first:
            filled, fill_price, leftover_order_id = await self._try_passive_sell(
                token_id, size_shares, bid_price, loop
            )
            if filled:
                logger.info(
                    "EXIT [%s] via passive maker: %.4f shares @ $%.4f",
                    exit_type, size_shares, fill_price,
                )
                return True, fill_price
            # Cancel unfilled maker order before going IOC
            if leftover_order_id:
                try:
                    await loop.run_in_executor(
                        None, lambda: self._client.cancel(leftover_order_id)
                    )
                    logger.info("Cancelled unfilled passive sell %s → switching to IOC", leftover_order_id)
                except Exception as exc:
                    logger.warning("Could not cancel passive order %s: %s", leftover_order_id, exc)

        # ── IOC exit — chunked if book is thin ───────────────────────────
        if allow_chunks and avail_shares < size_shares * _CHUNK_THIN_RATIO:
            return await self._chunked_ioc_exit(
                token_id, size_shares, bid_price, floor_price, avail_shares, exit_type, loop
            )
        else:
            ok, price, _ = await self._ioc_sell(token_id, size_shares, floor_price, loop)
            if ok:
                return True, price
            # IOC got zero fill — FAK orders free the balance immediately but the
            # API-side lock can persist for ~1-2s. Wait, re-query the real balance,
            # then retry with a wider floor price.
            wider_floor = max(0.01, round(floor_price - max_slippage, 4))
            logger.warning(
                "IOC sell zero fill at $%.4f — waiting 1.5s for balance unlock, "
                "then retrying at wider floor $%.4f",
                floor_price, wider_floor,
            )
            await asyncio.sleep(1.5)   # let API-side balance lock expire
            # Re-query actual balance (may have settled to a slightly different value)
            fresh_balance = await self._get_token_balance(token_id, loop)
            if fresh_balance < 0.01:
                logger.info("Position already closed after FAK zero-fill settlement — no retry needed")
                return True, bid_price
            size_shares = round(fresh_balance, 4)  # sell exactly what we hold
            ok2, price2, _ = await self._ioc_sell(token_id, size_shares, wider_floor, loop)
            return ok2, price2 if ok2 else bid_price

    async def _chunked_ioc_exit(
        self,
        token_id: str,
        size_shares: float,
        bid_price: float,
        floor_price: float,
        avail_shares: float,
        exit_type: str,
        loop,
    ) -> tuple[bool, float]:
        """
        Exit in two chunks when available bid depth < position size.
        Sells what the book can absorb now, waits for replenishment, sells rest.
        """
        chunk1 = min(avail_shares, size_shares)
        chunk2 = size_shares - chunk1

        logger.info(
            "CHUNKED EXIT [%s]: chunk1=%.4f chunk2=%.4f (book thin at $%.4f)",
            exit_type, chunk1, chunk2, floor_price,
        )

        total_sold    = 0.0
        weighted_usdc = 0.0

        if chunk1 > 0.001:
            ok1, price1, sold1 = await self._ioc_sell(token_id, chunk1, floor_price, loop)
            if ok1 and sold1 > 0.001:
                total_sold    += sold1
                weighted_usdc += price1 * sold1

        if chunk2 > 0.001:
            logger.info(
                "Chunk 1 done (%.4f shares) — waiting %.1fs for book to replenish",
                total_sold, _CHUNK_WAIT_SECONDS,
            )
            await asyncio.sleep(_CHUNK_WAIT_SECONDS)

            # Re-query live bid after wait
            avail2, _ = await _fetch_bid_depth_shares(token_id, floor_price)
            remaining = await self._get_token_balance(token_id, loop)  # actual remaining
            if remaining > 0.001:
                ok2, price2, sold2 = await self._ioc_sell(token_id, remaining, floor_price, loop)
                if ok2 and sold2 > 0.001:
                    total_sold    += sold2
                    weighted_usdc += price2 * sold2

        if total_sold > 0.001:
            avg_price = weighted_usdc / total_sold
            logger.info(
                "CHUNKED EXIT complete: %.4f shares @ avg $%.4f ($%.2f total)",
                total_sold, avg_price, weighted_usdc,
            )
            return True, avg_price

        logger.critical(
            "CHUNKED EXIT failed: 0 shares sold | token=%s bid=%.4f floor=%.4f",
            token_id[:16], bid_price, floor_price,
        )
        return False, bid_price

    async def _ioc_sell(
        self,
        token_id: str,
        shares_to_sell: float,
        floor_price: float,
        loop,
    ) -> tuple[bool, float, float]:
        """
        Post an IOC limit sell at floor_price.
        Fills at any bid >= floor_price; cancels remainder immediately.
        Returns (success, exit_price, shares_sold).
        """
        # ── Pre-flight validation ───────────────────────────────────────────────
        # Round to 4 decimal places (matching Polymarket’s 4-dp price tick).
        # Using 6dp causes IEEE 754 drift: e.g. 21.7391 becomes
        # 21.739100000000003 in float64, which makes the SDK’s
        # int(size * 1e6 * price) truncate to the wrong integer.
        size_sell = round(shares_to_sell, 4)
        # Minimum USDC floor: if price × size ≤ 0 the API returns
        # "maker and taker amount must be higher than 0".
        usdc_value = round(size_sell * floor_price, 6)
        if size_sell <= 0 or usdc_value <= 0:
            logger.error(
                "IOC sell pre-flight FAILED: size=%.6f floor=%.4f usdc_value=%.6f — "
                "skipping order (would produce invalid API amounts)",
                shares_to_sell, floor_price, usdc_value,
            )
            return False, 0.0, 0.0
        logger.debug(
            "IOC sell pre-flight OK: size=%.4f floor=%.4f usdc_value=%.4f",
            size_sell, floor_price, usdc_value,
        )
        order_args = OrderArgs(
            token_id=token_id,
            price=floor_price,
            size=size_sell,      # 4dp — clean integer conversion in SDK
            side="SELL",
        )
        options = PartialCreateOrderOptions()
        try:
            balance_before = await self._get_token_balance(token_id, loop)
            signed = await self._sdk_call(
                lambda: self._client.create_order(order_args, options)
            )
            result = await self._sdk_call(
                lambda: self._client.post_order(signed, OrderType.FAK)
            )
            _check_response(result, f"ioc_sell@{floor_price:.4f}")
            await asyncio.sleep(0.3)   # brief settlement pause

            balance_after = await self._get_token_balance(token_id, loop)
            shares_sold   = max(0.0, balance_before - balance_after)

            if shares_sold > 0.001:
                actual_price = floor_price
                order_id = (result or {}).get("orderID") or (result or {}).get("id", "") if result else ""
                if order_id:
                    try:
                        await self._rate_limiter.acquire()
                        status = await loop.run_in_executor(
                            None, lambda: self._client.get_order(order_id)
                        )
                        avg_p = float(
                            (status or {}).get("averagePrice") or
                            (status or {}).get("avgPrice") or
                            (status or {}).get("price") or 0
                        )
                        if avg_p > 0:
                            actual_price = avg_p
                    except Exception as exc:
                        logger.debug("get_order fill price fallback failed: %s", exc)

                logger.info(
                    "IOC sell confirmed: %.4f shares @ $%.4f (balance %.4f → %.4f)",
                    shares_sold, actual_price, balance_before, balance_after,
                )
                return True, actual_price, shares_sold

            logger.warning(
                "IOC sell: no fill (balance %.4f → %.4f) | floor=%.4f",
                balance_before, balance_after, floor_price,
            )
            return False, 0.0, 0.0

        except Exception as exc:
            logger.error("IOC sell failed: %s", exc)
            return False, 0.0, 0.0

    async def _try_passive_sell(
        self,
        token_id: str,
        size_shares: float,
        bid_price: float,
        loop,
    ) -> tuple[bool, float, str]:
        """
        Post a GTC limit sell AT bid_price (passive maker).
        Waits _MAKER_TRY_SECONDS. Returns (filled, fill_price, order_id_if_unfilled).
        Caller must cancel order_id if not filled.
        """
        order_args = OrderArgs(
            token_id=token_id,
            price=bid_price,
            size=round(size_shares, 4),  # 4dp — matches Polymarket tick precision
            side="SELL",
        )
        order_id = ""
        try:
            balance_before = await self._get_token_balance(token_id, loop)
            result = await self._sdk_call(
                lambda: self._client.create_and_post_order(
                    order_args, PartialCreateOrderOptions()
                )
            )
            _check_response(result, "passive_sell")
            order_id = (result or {}).get("orderID") or (result or {}).get("id", "")
            logger.info(
                "Passive sell posted: %.4f shares @ $%.4f | order=%s | waiting %.1fs",
                size_shares, bid_price, order_id, _MAKER_TRY_SECONDS,
            )
        except Exception as exc:
            logger.warning("Passive sell order failed: %s", exc)
            return False, 0.0, ""

        await asyncio.sleep(_MAKER_TRY_SECONDS)

        balance_after = await self._get_token_balance(token_id, loop)
        shares_sold   = max(0.0, balance_before - balance_after)

        if shares_sold > 0.001:
            actual_price = bid_price
            if order_id:
                try:
                    await self._rate_limiter.acquire()
                    status = await loop.run_in_executor(
                        None, lambda: self._client.get_order(order_id)
                    )
                    avg_p = float(
                        (status or {}).get("averagePrice") or
                        (status or {}).get("avgPrice") or
                        (status or {}).get("price") or 0
                    )
                    if avg_p > 0:
                        actual_price = avg_p
                except Exception as exc:
                    logger.debug("get_order fill price fallback failed: %s", exc)

            logger.info(
                "Passive sell filled: %.4f shares @ $%.4f", shares_sold, actual_price
            )
            return True, actual_price, ""   # filled — no pending order to cancel

        logger.info(
            "Passive sell unfilled after %.1fs — caller will cancel and switch to IOC",
            _MAKER_TRY_SECONDS,
        )
        return False, 0.0, order_id   # caller must cancel this

    async def _get_token_balance(self, token_id: str, loop) -> float:
        """
        Return on-chain conditional token balance in shares (handles 1e6 decimals).

        Uses integer division (// 1) then divides by 1_000_000 (Python int, not
        float 1e6) to avoid IEEE 754 drift.  Result is rounded to 4 decimal
        places so it feeds cleanly into 4dp OrderArgs.size without carrying
        floating-point noise downstream.
        """
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            await self._rate_limiter.acquire()
            resp = await loop.run_in_executor(
                None,
                lambda: self._client.get_balance_allowance(
                    params=BalanceAllowanceParams(
                        asset_type=AssetType.CONDITIONAL, token_id=token_id
                    )
                ),
            )
            raw = int((resp or {}).get("balance", 0))   # micro-shares (integer)
            return round(raw / 1_000_000, 4)             # 4dp float — no float drift
        except Exception as exc:
            logger.warning("Token balance query failed: %s", exc)
            return 0.0

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
                    await self._rate_limiter.acquire()
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
                result = await self._sdk_call(

                    lambda: self._client.create_and_post_order(

                        order_args, PartialCreateOrderOptions()

                    )

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
                    actual_price = current_price
                    if prev_order_id:
                        try:
                            await self._rate_limiter.acquire()
                            status = await loop.run_in_executor(
                                None, lambda: self._client.get_order(prev_order_id)
                            )
                            avg_p = float(
                                (status or {}).get("averagePrice") or
                                (status or {}).get("avgPrice") or
                                (status or {}).get("price") or 0
                            )
                            if avg_p > 0:
                                actual_price = avg_p
                        except Exception as exc:
                            logger.debug("get_order fill price fallback failed: %s", exc)

                    logger.info(
                        "SELL CONFIRMED on attempt %d: balance=%.6f (dust) | actual_exit_price=%.4f",
                        attempt, remaining, actual_price,
                    )
                    return True, actual_price
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

    async def _place_ioc_limit_order(
        self,
        token_id: str,
        side: str,
        amount_usdc: float,
        signal_price: float,
        max_price: float,
        loop,
        fee_bps: int = 0,
    ) -> dict | None:
        """
        IOC limit order at max_price (signal_ask + MAX_SLIPPAGE).
        Fills only at prices <= max_price; cancels any unfilled remainder.
        This prevents book-sweeping that destroys average entry price.
        """
        # ── Precision-safe amounts — integer GCD arithmetic ──────────────────
        # The SDK internally computes makerAmount = round(max_price × takerAmount)
        # where takerAmount = round(size × 1e6).  Simple float rounding can't
        # guarantee that product has ≤ 2dp.  _safe_order_amounts() uses GCD
        # arithmetic to find the LARGEST share count where the product is exact.
        size_shares, amount_usdc = self._safe_order_amounts(amount_usdc, max_price)
        if size_shares <= 0:
            logger.error(
                "IOC pre-flight: no valid share quantity for price=%.4f "
                "budget=$%.2f — order skipped",
                max_price, amount_usdc,
            )
            return None
        logger.debug(
            "IOC precision: signal=$%.4f max=$%.4f → size=%.4f shares maker=$%.2f",
            signal_price, max_price, size_shares, amount_usdc,
        )
        order_args  = OrderArgs(
            token_id=token_id,
            price=max_price,
            size=size_shares,
            side=side,
            fee_rate_bps=fee_bps,
        )
        options = PartialCreateOrderOptions()
        try:
            # Snapshot balance BEFORE the fill so we can compute the exact delta
            _bal_before = 0.0
            try:
                from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                await self._rate_limiter.acquire()
                _b = await loop.run_in_executor(
                    None,
                    lambda: self._client.get_balance_allowance(
                        params=BalanceAllowanceParams(
                            asset_type=AssetType.CONDITIONAL, token_id=token_id
                        )
                    ),
                )
                _bal_before = int((_b or {}).get("balance", 0)) / 1_000_000
            except Exception:
                pass  # non-fatal; falls back to absolute balance below

            # Two-step: create signed limit order, then post as IOC
            signed_order = await self._sdk_call(
                lambda: self._client.create_order(order_args, options)
            )
            logger.info(
                "IOC limit order signed: %.4f shares @ max $%.4f (signal $%.4f)",
                size_shares, max_price, signal_price,
            )
            result = await self._sdk_call(
                lambda: self._client.post_order(signed_order, OrderType.FAK)
            )
            _check_response(result, "ioc_limit")
            order_id = (result or {}).get("orderID") or (result or {}).get("id", "") if result else ""

            # ── Verify actual fill via on-chain token balance delta ────────────
            # CRITICAL: read balance AFTER fill and subtract the pre-order snapshot.
            # Reading absolute balance would include tokens already held from prior
            # trades on the same market, producing a wildly wrong price (e.g. $4.36).
            actual_price:  float | None = None
            actual_shares: float | None = None
            fill_usdc:     float        = 0.0

            try:
                from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                await self._rate_limiter.acquire()
                resp = await loop.run_in_executor(
                    None,
                    lambda: self._client.get_balance_allowance(
                        params=BalanceAllowanceParams(
                            asset_type=AssetType.CONDITIONAL, token_id=token_id
                        )
                    ),
                )
                bal_after   = int((resp or {}).get("balance", 0)) / 1_000_000
                # Use delta (after - before) so we see only what THIS order filled.
                # Absolute balance includes tokens held from prior trades on the same
                # market, which would inflate shares_held and produce $4.36-style prices.
                delta_shares = round(max(0.0, bal_after - _bal_before), 6)
                if delta_shares > 0.001:
                    actual_shares = delta_shares
                    actual_price  = round(amount_usdc / delta_shares, 6)
                    fill_usdc     = amount_usdc          # exact GCD-computed USDC spent
                    logger.info(
                        "IOC fill verified: Δ%.4f shares (%.4f→%.4f) → $%.4f avg = $%.2f USDC",
                        delta_shares, _bal_before, bal_after, actual_price, fill_usdc,
                    )
                elif bal_after > 0.001 and _bal_before == 0.0:
                    # Pre-snapshot failed — only safe to use absolute if we had nothing before
                    actual_shares = round(bal_after, 6)
                    actual_price  = round(amount_usdc / actual_shares, 6)
                    fill_usdc     = amount_usdc
                    logger.info(
                        "IOC fill (abs fallback): %.4f shares → $%.4f avg = $%.2f USDC",
                        actual_shares, actual_price, fill_usdc,
                    )
            except Exception as exc:
                logger.debug("Balance verification after IOC failed: %s", exc)

            # Fallback: get_order for fill price
            if not actual_price and order_id:
                try:
                    await self._rate_limiter.acquire()
                    status = await loop.run_in_executor(
                        None, lambda: self._client.get_order(order_id)
                    )
                    actual_price = float(
                        (status or {}).get("averagePrice") or
                        (status or {}).get("avgPrice") or
                        (status or {}).get("price") or 0
                    ) or None
                    if actual_price:
                        logger.info(
                            "IOC fill price from get_order: $%.4f (status=%s)",
                            actual_price, (status or {}).get("status"),
                        )
                except Exception as exc:
                    logger.debug("get_order fill price fallback failed: %s", exc)

            if actual_price and signal_price:
                slip_pct = ((actual_price - signal_price) / signal_price) * 100
                if abs(slip_pct) >= 5.0:
                    logger.warning(
                        "HIGH SLIPPAGE on IOC: signal=$%.4f actual=$%.4f (%+.1f%%)",
                        signal_price, actual_price, slip_pct,
                    )
                else:
                    logger.info(
                        "IOC slippage: signal=$%.4f cap=$%.4f actual=$%.4f (%+.1f%%)",
                        signal_price, max_price, actual_price, slip_pct,
                    )

            # Tiny fill guard — flag for caller to handle
            if fill_usdc > 0 and fill_usdc < _MIN_FILL_USDC:
                logger.warning(
                    "TINY FILL: IOC settled only $%.2f (%.4f shares @ $%.4f) "
                    "vs intended $%.2f — order book was shallower than depth check indicated",
                    fill_usdc, actual_shares or 0, actual_price or 0, amount_usdc,
                )
                if result:
                    result["tiny_fill"] = True

            if result:
                if actual_price  is not None: result["actual_fill_price"]  = actual_price
                if actual_shares is not None: result["actual_fill_shares"] = actual_shares
                result["actual_fill_usdc"] = fill_usdc

            await self.verify_trade(order_id)
            return result

        except Exception as exc:
            logger.error("IOC limit order failed: %s", exc)
            return None

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
            signed_order = await self._sdk_call(
                lambda: self._client.create_market_order(market_args, options)
            )
            # Pre-execution quote price from signed order (approximate — replaced by fill data below)
            pre_exec_price = None
            try:
                pre_exec_price = float(
                    getattr(signed_order, "price", None)
                    or (signed_order.get("price") if isinstance(signed_order, dict) else None)
                    or 0
                ) or None
            except Exception:
                pass
            logger.info("Signed market order created (pre-exec quote=%.4f): %s", pre_exec_price or 0, signed_order)

            result = await self._sdk_call(
                lambda: self._client.post_order(signed_order, OrderType.FOK)
            )
            _check_response(result, "market")
            order_id = result.get("orderID") or result.get("id", "") if result else ""

            # ── Verify actual fill price from exchange ─────────────────────
            actual_price: float | None = None

            # Priority 1: query order status — most reliable source of fill price
            if order_id:
                try:
                    await self._rate_limiter.acquire()
                    status = await loop.run_in_executor(None, lambda: self._client.get_order(order_id))
                    if status:
                        actual_price = float(
                            (status.get("averagePrice") or 0)
                            or (status.get("avgPrice") or 0)
                            or (status.get("price") or 0)
                        ) or None
                        if actual_price:
                            logger.info(
                                "Fill price from get_order: $%.4f (order_id=%s status=%s)",
                                actual_price, order_id, status.get("status"),
                            )
                except Exception as exc:
                    logger.debug("get_order fill price query failed: %s", exc)

            # Priority 2: token balance delta — calculate shares received, back-compute price
            if not actual_price and amount_usdc > 0:
                try:
                    from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                    resp = await loop.run_in_executor(
                        None,
                        lambda: self._client.get_balance_allowance(
                            params=BalanceAllowanceParams(
                                asset_type=AssetType.CONDITIONAL, token_id=token_id
                            )
                        ),
                    )
                    shares_held = int((resp or {}).get("balance", 0)) / 1e6
                    if shares_held > 0.01:
                        actual_price = round(amount_usdc / shares_held, 6)
                        logger.info(
                            "Fill price from balance: shares=%.4f → price=%.4f (USDC=%.2f)",
                            shares_held, actual_price, amount_usdc,
                        )
                except Exception as exc:
                    logger.debug("Balance fill price query failed: %s", exc)

            # Priority 3: fall back to result fields, then pre-execution quote
            if not actual_price:
                actual_price = (
                    float((result or {}).get("price") or 0)
                    or float((result or {}).get("averagePrice") or 0)
                    or pre_exec_price
                ) or None

            if actual_price and pre_exec_price:
                slippage_pct = ((actual_price - pre_exec_price) / pre_exec_price) * 100
                logger.info(
                    "Slippage: pre-exec=%.4f  actual_fill=%.4f  delta=%+.1f%%",
                    pre_exec_price, actual_price, slippage_pct,
                )

            await self.verify_trade(order_id)
            if result and actual_price:
                result["actual_fill_price"] = actual_price
            return result
        except Exception as exc:
            logger.error("Market order failed: %s", exc)
            return None
