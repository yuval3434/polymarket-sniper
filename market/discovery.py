import asyncio
import json
import logging
import time
from datetime import datetime, timezone

import aiohttp

from config import GAMMA_API_BASE, RECONNECT_BACKOFF_INITIAL
from market.state import MarketState

logger = logging.getLogger(__name__)

FIVE_MINUTES = 300  # seconds


def _next_5min_boundary_ts() -> int:
    """Return the Unix timestamp of the NEXT 5-minute UTC boundary."""
    now = int(time.time())
    remainder = now % FIVE_MINUTES
    return now + (FIVE_MINUTES - remainder)


def _current_5min_boundary_ts() -> int:
    """Return the Unix timestamp of the CURRENT (most recent) 5-minute UTC boundary."""
    now = int(time.time())
    return now - (now % FIVE_MINUTES)


async def _fetch_market(session: aiohttp.ClientSession, slug: str) -> dict | None:
    url = f"{GAMMA_API_BASE}/markets"
    params = {"slug": slug}
    for attempt in range(3):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # Gamma returns a list; find exact slug match
                    markets = data if isinstance(data, list) else data.get("markets", [])
                    for m in markets:
                        if m.get("slug") == slug:
                            return m
                    logger.warning("Slug %s not found in Gamma response", slug)
                else:
                    logger.warning("Gamma API returned %s for slug %s", resp.status, slug)
        except Exception as exc:
            logger.warning("Market discovery attempt %d failed: %s", attempt + 1, exc)
        await asyncio.sleep(RECONNECT_BACKOFF_INITIAL * (2 ** attempt))
    return None


def _parse_market(data: dict, state: MarketState) -> bool:
    """Populate state from a Gamma market dict. Returns True on success."""
    try:
        clob_ids = data.get("clobTokenIds") or data.get("clob_token_ids", [])
        # Gamma API sometimes returns this as a JSON-encoded string
        if isinstance(clob_ids, str):
            clob_ids = json.loads(clob_ids)
        if len(clob_ids) < 2:
            logger.error("Expected 2 clobTokenIds, got: %s", clob_ids)
            return False

        end_date = data.get("endDate") or data.get("end_date_iso") or data.get("endDateIso")
        if end_date:
            # ISO 8601 string → Unix timestamp
            dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            expiry_ts = dt.timestamp()
        else:
            expiry_ts = float(_next_5min_boundary_ts())

        state.market_id = str(data.get("id", ""))
        state.condition_id = data.get("conditionId", "") or data.get("condition_id", "")
        state.up_token_id = str(clob_ids[0])
        state.down_token_id = str(clob_ids[1])
        state.expiry_ts = expiry_ts
        state.reset_for_new_market()

        logger.info(
            "Market loaded: id=%s up=%s down=%s expiry=%s",
            state.market_id,
            state.up_token_id,
            state.down_token_id,
            datetime.fromtimestamp(expiry_ts, tz=timezone.utc).isoformat(),
        )
        return True
    except Exception as exc:
        logger.error("Failed to parse market data: %s | data=%s", exc, data)
        return False


async def discovery_loop(state: MarketState) -> None:
    """
    Runs continuously. Discovers the active 5-minute BTC market and
    refreshes state at each 5-minute boundary.
    """
    async with aiohttp.ClientSession() as session:
        while True:
            # Try current boundary first, then next
            for boundary_fn in (_current_5min_boundary_ts, _next_5min_boundary_ts):
                ts = boundary_fn()
                slug = f"btc-updown-5m-{ts}"
                logger.info("Querying market slug: %s", slug)
                data = await _fetch_market(session, slug)
                if data:
                    _parse_market(data, state)
                    break
            else:
                logger.error("Could not discover active BTC 5-min market — will retry next cycle")

            # Sleep until 1 second after the next 5-minute boundary
            next_boundary = _next_5min_boundary_ts()
            sleep_for = max(1.0, next_boundary - time.time() + 1.0)
            logger.debug("Next discovery in %.1fs", sleep_for)
            await asyncio.sleep(sleep_for)
