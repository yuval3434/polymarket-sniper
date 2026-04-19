"""
Chainlink BTC/USD price feed — replaces Binance stream.

Polls the Chainlink BTC/USD aggregator on Polygon every POLL_INTERVAL seconds.
Feed address: 0xc907E116054Ad103354f2D350FD2514433D57F6F (Polygon mainnet)
Answer decimals: 8  (divide raw answer by 1e8 to get USD price)
Chainlink updates ~every 27s or when price moves >0.5%.

Recovery flow (mirrors Binance stream):
  - On RPC failure: exponential backoff, try fallback RPCs
  - On kill switch "chainlink": accumulate _STABLE_TICKS consecutive good polls
  - After _STABLE_TICKS: disengage kill switch, resume trading
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque

from config import POLYGON_RPC_URL, VOL_WINDOW_SECONDS

logger = logging.getLogger(__name__)

# Chainlink BTC/USD aggregator on Polygon
_FEED_ADDRESS = "0xc907E116054Ad103354f2D350FD2514433D57F6F"
_DECIMALS = 8

# Minimal ABI — only latestRoundData needed
_ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"name": "roundId",        "type": "uint80"},
            {"name": "answer",         "type": "int256"},
            {"name": "startedAt",      "type": "uint256"},
            {"name": "updatedAt",      "type": "uint256"},
            {"name": "answeredInRound","type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]

# Public Polygon RPC fallbacks (tried in order)
_RPC_URLS = [
    POLYGON_RPC_URL,
    "https://polygon-bor-rpc.publicnode.com",
    "https://rpc.ankr.com/polygon",
    "https://1rpc.io/matic",
]

POLL_INTERVAL   = 2.0    # seconds between Chainlink reads (must be < kill switch threshold)
_BACKOFF_INITIAL = 2.0
_BACKOFF_MAX     = 30.0
_STABLE_TICKS    = 3     # consecutive good polls before re-enabling trading

_MAX_DEQUE_LEN    = VOL_WINDOW_SECONDS * int(1 / POLL_INTERVAL * 60)  # ~6 min of ticks
_VOL_RECALC_TICKS = 10   # recompute vol every N polls (cheap at 5s intervals)

# Max age of Chainlink answer before we treat it as stale (5 minutes)
_MAX_ANSWER_AGE_S = 300


def _annualized_vol(prices: deque) -> float:
    if len(prices) < 5:
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
    ticks_per_year = (1 / POLL_INTERVAL) * 60 * 60 * 24 * 365
    return std_per_tick * math.sqrt(ticks_per_year)


def _make_contract(rpc_url: str):
    """Create a web3 contract instance for the given RPC URL."""
    from web3 import Web3
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 8}))
    return w3.eth.contract(
        address=Web3.to_checksum_address(_FEED_ADDRESS),
        abi=_ABI,
    )


def _fetch_price(contract) -> tuple[float, int]:
    """
    Call latestRoundData. Returns (price_usd, updated_at_unix).
    Raises on any RPC/contract error.
    """
    data = contract.functions.latestRoundData().call()
    # data = (roundId, answer, startedAt, updatedAt, answeredInRound)
    answer    = data[1]
    updated_at = data[3]
    price = answer / (10 ** _DECIMALS)
    return price, updated_at


async def chainlink_stream_loop(state) -> None:  # type: ignore[type-arg]
    """
    Main entry point. Runs forever — polls Chainlink every POLL_INTERVAL seconds.
    Tries each RPC URL in sequence on failure, with exponential backoff.
    """
    price_window: deque = deque(maxlen=_MAX_DEQUE_LEN)
    backoff = _BACKOFF_INITIAL
    tick_count = 0
    stable_count = 0
    current_rpc_idx = 0

    # Build contracts lazily (web3 import may fail if not installed)
    contracts = []
    try:
        for url in _RPC_URLS:
            if url:
                try:
                    contracts.append(_make_contract(url))
                except Exception as exc:
                    logger.warning("chainlink: could not init RPC %s — %s", url, exc)
        if not contracts:
            raise RuntimeError("No usable Polygon RPC URLs — install web3 and set POLYGON_RPC_URL")
    except ImportError:
        logger.critical("chainlink: web3 package not installed — run: pip install web3")
        return

    logger.info(
        "Chainlink BTC/USD stream started (%d RPCs, poll=%.0fs)",
        len(contracts), POLL_INTERVAL,
    )

    while True:
        contract = contracts[current_rpc_idx % len(contracts)]
        try:
            price, updated_at = await asyncio.to_thread(_fetch_price, contract)

            # Sanity-check: reject answers older than _MAX_ANSWER_AGE_S
            answer_age = time.time() - updated_at
            if answer_age > _MAX_ANSWER_AGE_S:
                logger.warning(
                    "chainlink: answer is %.0fs old (updatedAt=%d) — skipping tick",
                    answer_age, updated_at,
                )
                await asyncio.sleep(POLL_INTERVAL)
                continue

            if price <= 0:
                logger.warning("chainlink: zero/negative price — skipping tick")
                await asyncio.sleep(POLL_INTERVAL)
                continue

            state.btc_price = price
            state.btc_last_update = time.monotonic()

            if state.opening_price == 0.0 and state.up_token_id:
                state.opening_price = price
                logger.info("Opening price set from first Chainlink tick: %.2f", price)

            price_window.append(price)
            tick_count += 1

            if tick_count % _VOL_RECALC_TICKS == 0:
                state._vol_estimate = _annualized_vol(price_window)  # type: ignore[attr-defined]

            # Kill-switch recovery: accumulate stable polls
            if state.kill_switch_active and state.kill_switch_source == "chainlink":
                stable_count += 1
                if stable_count >= _STABLE_TICKS:
                    state.kill_switch_active = False
                    state.kill_switch_source = ""
                    state.kill_switch_reason = ""
                    logger.info(
                        "Chainlink feed stabilised. Kill Switch disengaged. "
                        "Trading resumed after %d consecutive valid polls.",
                        stable_count,
                    )
            else:
                stable_count = 0

            backoff = _BACKOFF_INITIAL  # reset on success
            logger.debug("chainlink: BTC=%.2f  age=%.0fs  vol=%.4f",
                         price, answer_age,
                         getattr(state, "_vol_estimate", 0.0))

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "chainlink: RPC error (idx=%d, backoff=%.0fs) — %s",
                current_rpc_idx, backoff, exc,
            )
            current_rpc_idx += 1  # rotate to next RPC
            if current_rpc_idx >= len(contracts):
                current_rpc_idx = 0
                logger.warning("chainlink: all RPCs failed — sleeping %.0fs before retry", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _BACKOFF_MAX)
                continue

        await asyncio.sleep(POLL_INTERVAL)
