import asyncio
import json
import logging
import math
import os
import socket
import time
from collections import deque

import aiohttp

from config import BINANCE_WS_URL, VOL_WINDOW_SECONDS
from market.state import MarketState

logger = logging.getLogger(__name__)

# Primary URL from config/.env; fallback tries alternate port
_WS_URLS = [
    BINANCE_WS_URL,
    "wss://stream.binance.com:443/ws/btcusdt@aggTrade",
    "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
]
_REST_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
_REST_POLL_INTERVAL = 0.2

_BACKOFF_INITIAL  = 1.0
_BACKOFF_MAX      = 10.0
_STABLE_TICKS     = 3       # consecutive good ticks required before re-enabling trading
_MAX_DEQUE_LEN    = VOL_WINDOW_SECONDS * 15
_VOL_RECALC_TICKS = 50      # only recompute vol every N ticks — keeps event loop free
_WS_KEEPALIVE_S   = 3.0     # send explicit PING every 3s (detect silent drops fast)


def _annualized_vol(prices: deque) -> float:
    if len(prices) < 10:
        return 0.0
    log_returns = [
        math.log(prices[i] / prices[i - 1])
        for i in range(1, len(prices))
        if prices[i - 1] > 0
    ]
    if not log_returns:
        return 0.0
    n = len(log_returns)
    mean = sum(log_returns) / n
    variance = sum((r - mean) ** 2 for r in log_returns) / max(n - 1, 1)
    std_per_tick = math.sqrt(variance)
    ticks_per_year = 10 * 60 * 60 * 24 * 365
    return std_per_tick * math.sqrt(ticks_per_year)


async def binance_stream_loop(state: MarketState) -> None:
    """
    Main entry point. Runs forever — never returns unless the process exits.

    Recovery flow:
      disconnect / stale → soft kill switch fires (trading paused, orders cancelled)
      → exponential-backoff reconnect loop
      → on reconnect: buffer _STABLE_TICKS ticks
      → disengage kill switch, resume trading
    """
    price_window: deque = deque(maxlen=_MAX_DEQUE_LEN)
    backoff = _BACKOFF_INITIAL
    rest_fail_count = 0
    state._tick_count = 0  # type: ignore[attr-defined]

    while True:
        connected = await _try_websocket(state, price_window)
        if connected:
            backoff = _BACKOFF_INITIAL   # reset on clean run
        else:
            # WebSocket failed — escalate to REST fallback after repeated failures
            rest_fail_count += 1
            if rest_fail_count >= 3:
                logger.warning("WS unavailable — switching to REST fallback")
                await _rest_poll_loop(state, price_window)
                rest_fail_count = 0
                continue

            logger.warning("Binance WS disconnected — retrying in %.0fs", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _BACKOFF_MAX)


async def _try_websocket(state: MarketState, price_window: deque) -> bool:
    """
    Connect, stream, and handle messages. Returns True if at least one message
    was received (clean session), False on immediate/early failure.

    On disconnect: triggers soft kill switch, waits for stable buffer before
    re-enabling trading.
    """
    for url in _WS_URLS:
        try:
            connector = aiohttp.TCPConnector(enable_cleanup_closed=True)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.ws_connect(
                    url,
                    heartbeat=_WS_KEEPALIVE_S,   # aiohttp sends WS ping every 3s
                    receive_timeout=8.0,           # drop connection if silent for 8s (AWS NAT fast detection)
                    ssl=True,
                ) as ws:
                    # AWS NAT keepalive: tell the OS to probe the TCP connection
                    # every 10s after 10s idle — prevents silent NAT teardown
                    try:
                        sock = ws._writer.transport.get_extra_info("socket")
                        if sock is not None:
                            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                            if hasattr(socket, "TCP_KEEPIDLE"):   # Linux only
                                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 10)
                                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
                                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
                    except Exception:
                        pass  # non-fatal — WS-level pings are still active
                    state.btc_last_update = time.monotonic()
                    logger.info("Binance WebSocket connected (%s)", url)
                    received_any = False
                    stable_count = 0

                    # Parallel keepalive: proactively ping every 3s to detect silent drops.
                    # aiohttp's built-in heartbeat handles the WS protocol level;
                    # this task also monitors for no-data timeouts independently.
                    keepalive_task = asyncio.create_task(
                        _keepalive(ws, state, _WS_KEEPALIVE_S)
                    )
                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                _handle_agg_trade(msg.data, state, price_window)
                                received_any = True

                                if state.kill_switch_active and state.kill_switch_source == "binance":
                                    stable_count += 1
                                    if stable_count >= _STABLE_TICKS:
                                        state.kill_switch_active = False
                                        state.kill_switch_source = ""
                                        state.kill_switch_reason = ""
                                        logger.info(
                                            "Connection stabilized. Kill Switch disengaged. "
                                            "Trading resumed after %d consecutive valid ticks.",
                                            stable_count,
                                        )
                                else:
                                    stable_count = 0

                            elif msg.type == aiohttp.WSMsgType.PING:
                                await ws.pong()

                            elif msg.type == aiohttp.WSMsgType.PONG:
                                pass  # keepalive reply — connection confirmed alive

                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.warning("Binance WS error frame: %s", msg.data)
                                break

                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.CLOSED,
                            ):
                                logger.warning("Binance WS closed by server")
                                break
                    finally:
                        keepalive_task.cancel()

                    return received_any

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Binance WS error on %s: %s", url, exc)

    return False


async def _keepalive(
    ws: aiohttp.ClientWebSocketResponse,
    state: MarketState,
    interval: float,
) -> None:
    """
    Sends explicit PING frames every `interval` seconds.
    Also detects when messages have stopped arriving (silent drop) and
    logs a warning so the kill switch can fire cleanly.
    """
    try:
        while True:
            await asyncio.sleep(interval)
            if ws.closed:
                break
            try:
                await ws.ping()
            except Exception:
                break
            # Log if feed looks stale (informational — kill_switch.py fires the actual halt)
            age_ms = (time.monotonic() - state.btc_last_update) * 1000
            if age_ms > 2000:
                logger.warning(
                    "Binance WS keepalive: feed silent for %.0fms "
                    "(kill switch threshold is %.0fms)",
                    age_ms,
                    float(os.environ.get("KILL_SWITCH_LATENCY_MS", "5000")),
                )
    except asyncio.CancelledError:
        pass
    except Exception:
        pass


async def _rest_poll_loop(state: MarketState, price_window: deque) -> None:
    """REST fallback: poll every 200ms. Exits when WS becomes healthy again (kill switch cleared)."""
    logger.info("Binance REST fallback active — polling every %.0fms", _REST_POLL_INTERVAL * 1000)
    stable_count = 0

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(
                    _REST_URL, timeout=aiohttp.ClientTimeout(total=2)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = float(data["price"])
                        state.btc_price = price
                        state.btc_last_update = time.monotonic()
                        if state.opening_price == 0.0 and state.up_token_id:
                            state.opening_price = price
                        price_window.append(price)
                        state._vol_estimate = _annualized_vol(price_window)  # type: ignore[attr-defined]

                        if state.kill_switch_active and state.kill_switch_source == "binance":
                            stable_count += 1
                            if stable_count >= _STABLE_TICKS:
                                state.kill_switch_active = False
                                state.kill_switch_source = ""
                                state.kill_switch_reason = ""
                                logger.info(
                                    "REST feed stabilized. Kill Switch disengaged. Trading resumed."
                                )
                                return  # hand back to WS loop
                        else:
                            stable_count = 0

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Binance REST poll error: %s", exc)
                stable_count = 0

            await asyncio.sleep(_REST_POLL_INTERVAL)


def _handle_agg_trade(raw: str, state: MarketState, price_window: deque) -> None:
    try:
        msg = json.loads(raw)
        if msg.get("e") != "aggTrade":
            return
        price = float(msg["p"])
        state.btc_price = price
        state.btc_last_update = time.monotonic()
        if state.opening_price == 0.0 and state.up_token_id:
            state.opening_price = price
            logger.info("Opening price set from first Binance tick: %.2f", price)
        price_window.append(price)

        # Throttle vol calculation — O(n) CPU, must not run on every tick
        tick_count = getattr(state, "_tick_count", 0) + 1
        state._tick_count = tick_count  # type: ignore[attr-defined]
        if tick_count % _VOL_RECALC_TICKS == 0:
            state._vol_estimate = _annualized_vol(price_window)  # type: ignore[attr-defined]

    except (KeyError, ValueError, json.JSONDecodeError) as exc:
        logger.debug("Malformed Binance message: %s | %s", exc, raw[:120])
