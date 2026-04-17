import asyncio
import json
import logging
import time

import aiohttp

from config import (
    KILL_SWITCH_LATENCY_MS,
    POLY_HOST,
    POLY_WS_URL,
    RECONNECT_BACKOFF_INITIAL,
    RECONNECT_BACKOFF_MAX,
)
from market.state import MarketState

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL  = 2.0   # seconds between PING frames (must be < kill switch threshold)
_REST_POLL_INTERVAL = 1.0   # seconds between REST order book polls


async def polymarket_stream_loop(state: MarketState) -> None:
    """
    Runs two tasks in parallel:
      1. REST poller  — polls /book every 1s (reliable, always has data)
      2. WebSocket    — real-time updates when available
    """
    await asyncio.gather(
        _rest_book_loop(state),
        _ws_loop(state),
    )


async def _rest_book_loop(state: MarketState) -> None:
    """Poll Polymarket REST API for best bid/ask every second."""
    async with aiohttp.ClientSession() as session:
        last_logged_tokens: tuple[str, str] = ("", "")
        while True:
            if state.kill_switch_active:
                return
            up_id   = state.up_token_id
            down_id = state.down_token_id
            if up_id and down_id:
                try:
                    up_bid, up_ask     = await _fetch_best(session, up_id)
                    down_bid, down_ask = await _fetch_best(session, down_id)
                    if up_ask < 1.0 or down_ask < 1.0:
                        state.up_best_bid   = up_bid
                        state.up_best_ask   = up_ask
                        state.down_best_bid = down_bid
                        state.down_best_ask = down_ask
                        state.poly_last_update = time.monotonic()
                        if (up_id, down_id) != last_logged_tokens:
                            logger.info(
                                "Orderbook REST: UP bid=%.4f ask=%.4f | DOWN bid=%.4f ask=%.4f",
                                up_bid, up_ask, down_bid, down_ask,
                            )
                            last_logged_tokens = (up_id, down_id)
                except Exception as exc:
                    logger.debug("REST book poll error: %s", exc)
            await asyncio.sleep(_REST_POLL_INTERVAL)


async def _fetch_best(session: aiohttp.ClientSession, token_id: str) -> tuple[float, float]:
    """Return (best_bid, best_ask) for a single token via REST."""
    url = f"{POLY_HOST}/book"
    async with session.get(
        url, params={"token_id": token_id}, timeout=aiohttp.ClientTimeout(total=3)
    ) as resp:
        if resp.status != 200:
            return 0.0, 1.0
        data = await resp.json()
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        best_bid = max((float(b["price"]) for b in bids if b.get("price")), default=0.0)
        best_ask = min((float(a["price"]) for a in asks if a.get("price")), default=1.0)
        return best_bid, best_ask


async def _ws_loop(state: MarketState) -> None:
    """WebSocket for real-time updates (supplements REST)."""
    backoff = RECONNECT_BACKOFF_INITIAL
    subscribed_tokens: tuple[str, str] = ("", "")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    POLY_WS_URL,
                    heartbeat=HEARTBEAT_INTERVAL,
                    receive_timeout=HEARTBEAT_INTERVAL + 5,
                ) as ws:
                    state.poly_last_update = time.monotonic()
                    logger.info("Polymarket CLOB WebSocket connected")
                    backoff = RECONNECT_BACKOFF_INITIAL

                    if state.up_token_id and state.down_token_id:
                        await _subscribe(ws, state.up_token_id, state.down_token_id)
                        subscribed_tokens = (state.up_token_id, state.down_token_id)

                    ping_task = asyncio.create_task(_ping_loop(ws))
                    try:
                        async for msg in ws:
                            current = (state.up_token_id, state.down_token_id)
                            if current != subscribed_tokens and state.up_token_id:
                                await _subscribe(ws, state.up_token_id, state.down_token_id)
                                subscribed_tokens = current

                            if msg.type == aiohttp.WSMsgType.TEXT:
                                _handle_message(msg.data, state)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.warning("Polymarket WS error: %s", msg.data)
                                break
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.CLOSED,
                            ):
                                logger.warning("Polymarket WS closed")
                                break
                    finally:
                        ping_task.cancel()

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "Polymarket WS error: %s — reconnecting in %.1fs", exc, backoff
            )

        if state.kill_switch_active:
            return

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX)


async def _subscribe(
    ws: aiohttp.ClientWebSocketResponse, up_token: str, down_token: str
) -> None:
    payload = json.dumps(
        {"type": "subscribe", "asset_ids": [up_token, down_token]}
    )
    await ws.send_str(payload)
    logger.info("Subscribed to tokens: UP=%s DOWN=%s", up_token, down_token)


async def _ping_loop(ws: aiohttp.ClientWebSocketResponse) -> None:
    """Send PING every 10s to keep the connection alive."""
    try:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            await ws.send_str("PING")
    except asyncio.CancelledError:
        pass
    except Exception:
        pass


def _handle_message(raw: str, state: MarketState) -> None:
    state.poly_last_update = time.monotonic()

    if raw == "PONG":
        state.poly_last_update = time.monotonic()  # heartbeat counts as liveness proof
        return

    try:
        events = json.loads(raw)
    except json.JSONDecodeError:
        return

    if not isinstance(events, list):
        events = [events]

    for event in events:
        event_type = event.get("event_type") or event.get("type", "")

        if event_type == "book":
            _handle_book_snapshot(event, state)
        elif event_type in ("price_change", "tick_size_change"):
            _handle_price_change(event, state)
        elif event_type == "last_trade_price":
            pass  # logged elsewhere if needed
        elif event_type == "market":
            # Market-level updates; may contain book data
            _handle_book_snapshot(event, state)


def _handle_book_snapshot(event: dict, state: MarketState) -> None:
    """Rebuild best bid/ask from a full book snapshot."""
    asset_id = str(event.get("asset_id", ""))
    bids = event.get("bids", [])
    asks = event.get("asks", [])

    best_bid = max((float(b["price"]) for b in bids if b.get("price")), default=0.0)
    best_ask = min((float(a["price"]) for a in asks if a.get("price")), default=1.0)

    _update_side(asset_id, best_bid, best_ask, state)


def _handle_price_change(event: dict, state: MarketState) -> None:
    asset_id = str(event.get("asset_id", ""))
    price = event.get("price")
    side = event.get("side", "").upper()

    if price is None:
        return
    price_f = float(price)

    if asset_id == state.up_token_id:
        if side == "ASK":
            state.up_best_ask = price_f
        elif side == "BID":
            state.up_best_bid = price_f
    elif asset_id == state.down_token_id:
        if side == "ASK":
            state.down_best_ask = price_f
        elif side == "BID":
            state.down_best_bid = price_f


def _update_side(
    asset_id: str, best_bid: float, best_ask: float, state: MarketState
) -> None:
    if asset_id == state.up_token_id:
        state.up_best_bid = best_bid
        state.up_best_ask = best_ask
    elif asset_id == state.down_token_id:
        state.down_best_bid = best_bid
        state.down_best_ask = best_ask
