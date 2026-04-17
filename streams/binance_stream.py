import asyncio
import json
import logging
import math
import time
from collections import deque

import aiohttp

from config import (
    RECONNECT_BACKOFF_INITIAL,
    RECONNECT_BACKOFF_MAX,
    VOL_WINDOW_SECONDS,
)
from market.state import MarketState

logger = logging.getLogger(__name__)

# Port 443 works through most firewalls; 9443 is often blocked on Windows
_WS_URLS = [
    "wss://stream.binance.com:443/ws/btcusdt@aggTrade",
    "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
]
_REST_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
_REST_POLL_INTERVAL = 0.2   # seconds between REST fallback polls
_WS_FAIL_THRESHOLD = 5      # consecutive WS failures before switching to REST

_MAX_DEQUE_LEN = VOL_WINDOW_SECONDS * 15


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
    price_window: deque = deque(maxlen=_MAX_DEQUE_LEN)
    ws_fail_count = 0

    while True:
        if state.kill_switch_active:
            return

        if ws_fail_count >= _WS_FAIL_THRESHOLD:
            logger.warning(
                "WebSocket failed %d times — switching to REST polling fallback",
                ws_fail_count,
            )
            await _rest_poll_loop(state, price_window)
            return  # rest loop runs indefinitely unless kill switch fires

        success = await _try_websocket(state, price_window)
        if success:
            ws_fail_count = 0
        else:
            ws_fail_count += 1


async def _try_websocket(state: MarketState, price_window: deque) -> bool:
    """
    Attempt WebSocket connection. Returns True if it ran successfully
    (received at least one message), False on immediate failure.
    """
    backoff = RECONNECT_BACKOFF_INITIAL
    for url in _WS_URLS:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    url,
                    heartbeat=20,
                    receive_timeout=10.0,
                    ssl=True,
                ) as ws:
                    # Reset timestamp immediately so kill switch doesn't fire
                    # during the brief pause before the first message arrives
                    state.btc_last_update = time.monotonic()
                    logger.info("Binance WebSocket connected (%s)", url)
                    backoff = RECONNECT_BACKOFF_INITIAL
                    received_any = False

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            _handle_agg_trade(msg.data, state, price_window)
                            received_any = True
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.warning("Binance WS error: %s", msg.data)
                            break
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.CLOSED,
                        ):
                            logger.warning("Binance WS closed")
                            break

                    return received_any

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Binance WS error on %s: %s", url, exc)

    await asyncio.sleep(backoff)
    return False


async def _rest_poll_loop(state: MarketState, price_window: deque) -> None:
    """
    Fallback: poll Binance REST every 200ms when WebSocket is unavailable.
    Provides price data with slightly higher latency but works everywhere.
    """
    logger.info("Binance REST fallback active — polling every %.0fms", _REST_POLL_INTERVAL * 1000)
    async with aiohttp.ClientSession() as session:
        while True:
            if state.kill_switch_active:
                return
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
                            logger.info("Opening price set from REST fallback: %.2f", price)
                        price_window.append(price)
                        state._vol_estimate = _annualized_vol(price_window)  # type: ignore[attr-defined]
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Binance REST poll error: %s", exc)
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
        state._vol_estimate = _annualized_vol(price_window)  # type: ignore[attr-defined]
    except (KeyError, ValueError, json.JSONDecodeError) as exc:
        logger.debug("Malformed Binance message: %s | %s", exc, raw[:120])
