import asyncio
import logging
import time

from config import KILL_SWITCH_LATENCY_MS
from market.state import MarketState

logger = logging.getLogger(__name__)

_POLL_INTERVAL    = 0.1   # seconds between watchdog checks
_STALE_THRESHOLD  = KILL_SWITCH_LATENCY_MS / 1000.0


async def kill_switch_loop(
    state: MarketState,
    cancel_all_fn=None,
) -> None:
    """
    Polls feed freshness every 100ms.

    Soft Kill Switch behaviour (auto-recovery):
      - Sets state.kill_switch_active = True  → sniper_loop stops new orders
      - Sets state.kill_switch_source        → "binance" or "polymarket"
      - Calls cancel_all_fn() to cancel pending limit orders
      - Does NOT exit — the event loop keeps running
      - The stream loop clears kill_switch_active once the feed is stable again
        (after _STABLE_TICKS consecutive good ticks — see binance_stream.py)

    Hard Kill Switch (polymarket feed — no self-healing stream):
      - Same pause behaviour, but logged separately
    """
    while True:
        await asyncio.sleep(_POLL_INTERVAL)

        if state.kill_switch_active:
            continue  # already paused — stream loop will clear it when ready

        now = time.monotonic()
        binance_age_ms = (now - state.btc_last_update) * 1000
        poly_age_ms    = (now - state.poly_last_update) * 1000

        if binance_age_ms > KILL_SWITCH_LATENCY_MS:
            await _trigger(
                state, cancel_all_fn,
                source="binance",
                reason=f"Binance feed stale for {binance_age_ms:.0f}ms",
            )
        elif poly_age_ms > KILL_SWITCH_LATENCY_MS:
            await _trigger(
                state, cancel_all_fn,
                source="polymarket",
                reason=f"Polymarket feed stale for {poly_age_ms:.0f}ms",
            )


async def _trigger(
    state: MarketState,
    cancel_all_fn,
    source: str,
    reason: str,
) -> None:
    state.kill_switch_active = True
    state.kill_switch_source = source
    state.kill_switch_reason = reason

    logger.critical("KILL SWITCH TRIGGERED [%s]: %s", source.upper(), reason)
    logger.critical(
        "Trading paused. Auto-recovery active — waiting for %s feed to stabilise.",
        source,
    )

    if cancel_all_fn is not None:
        try:
            await cancel_all_fn()
            logger.info("Pending orders cancelled after kill switch")
        except Exception as exc:
            logger.error("cancel_all() failed: %s", exc)
