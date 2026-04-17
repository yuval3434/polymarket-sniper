import asyncio
import logging
import time

from config import KILL_SWITCH_LATENCY_MS
from market.state import MarketState

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 0.1  # seconds between watchdog checks
_STALE_THRESHOLD = KILL_SWITCH_LATENCY_MS / 1000.0  # convert ms → seconds


async def kill_switch_loop(
    state: MarketState,
    cancel_all_fn=None,
) -> None:
    """
    Polls feed freshness every 100ms. Triggers kill switch if either
    WebSocket feed goes silent for longer than KILL_SWITCH_LATENCY_MS.

    cancel_all_fn: optional async callable (e.g. clob_client.cancel_all)
    invoked once when the kill switch fires.
    """
    while True:
        await asyncio.sleep(_POLL_INTERVAL)

        if state.kill_switch_active:
            continue  # already fired; loop idles until restart

        now = time.monotonic()
        binance_age_ms = (now - state.btc_last_update) * 1000
        poly_age_ms = (now - state.poly_last_update) * 1000

        reason: str | None = None
        if binance_age_ms > KILL_SWITCH_LATENCY_MS:
            reason = f"Binance feed stale for {binance_age_ms:.0f}ms"
        elif poly_age_ms > KILL_SWITCH_LATENCY_MS:
            reason = f"Polymarket feed stale for {poly_age_ms:.0f}ms"

        if reason:
            state.kill_switch_active = True
            state.kill_switch_reason = reason
            logger.critical("KILL SWITCH TRIGGERED: %s", reason)
            logger.critical(
                "All new order placement halted. Restart process to reset."
            )

            if cancel_all_fn is not None:
                try:
                    await cancel_all_fn()
                    logger.info("cancel_all() completed after kill switch")
                except Exception as exc:
                    logger.error("cancel_all() failed after kill switch: %s", exc)
