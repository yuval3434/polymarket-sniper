"""
Ping Monitor — Network Health Diagnostic
=========================================
Measures round-trip latency to Binance REST API every second.
Correlates ping health with kill-switch events to distinguish
"my internet dropped" from "event loop was blocked".

Output  : ping_log.csv  (one row per second)
Log     : WARNING when RTT > 500ms or request fails
Columns : timestamp, rtt_ms, status, kill_switch_active, binance_feed_age_ms, loop_lag_ms
"""
from __future__ import annotations

import asyncio
import csv
import logging
import os
import time
from collections import deque

import aiohttp

from market.state import MarketState

logger = logging.getLogger(__name__)

_BASE     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CSV_PATH = os.path.join(_BASE, "ping_log.csv")

_PING_URL       = "https://api.binance.com/api/v3/ping"
_INTERVAL_S     = 1.0     # ping every second
_TIMEOUT_S      = 3.0     # treat as packet loss if no reply within 3s
_ROLLING_WINDOW = 60      # keep last 60 samples for stats

# Adaptive thresholds — computed from rolling baseline, not hardcoded.
# A spike is relative: RTT > SPIKE_MULTIPLIER × 60s-average = anomaly.
# Absolute WARN only fires if we have no baseline yet.
_SPIKE_MULTIPLIER   = 2.0    # RTT > 2× avg → network spike
_ABSOLUTE_WARN_MS   = 800    # always warn if RTT exceeds this regardless of avg
_MIN_SAMPLES_FOR_ADAPTIVE = 10  # need at least N samples before using adaptive logic

_FIELDNAMES = [
    "timestamp", "rtt_ms", "status",
    "kill_switch_active", "kill_switch_source",
    "binance_feed_age_ms", "loop_lag_ms",
    "rolling_avg_ms",
]


async def ping_monitor_loop(state: MarketState) -> None:
    """
    Runs forever alongside the main bot.
    Writes one CSV row per second and logs anomalies.

    How to interpret results:
      - RTT spikes   + kill switch fires  →  network is the culprit
      - RTT normal   + kill switch fires  →  event loop was blocked (code issue)
      - loop_lag_ms high                 →  event loop was blocked regardless of network
    """
    _ensure_header()
    rtt_window: deque[float] = deque(maxlen=_ROLLING_WINDOW)
    consecutive_fails = 0

    async with aiohttp.ClientSession() as session:
        while True:
            loop_start = time.monotonic()
            rtt_ms, status = await _ping(session)
            loop_elapsed = (time.monotonic() - loop_start) * 1000
            loop_lag_ms = max(0.0, loop_elapsed - rtt_ms)

            if rtt_ms >= 0:
                rtt_window.append(rtt_ms)
                consecutive_fails = 0
            else:
                consecutive_fails += 1

            rolling_avg = sum(rtt_window) / len(rtt_window) if rtt_window else 0.0
            binance_age_ms = (time.monotonic() - state.btc_last_update) * 1000

            _write_row(
                rtt_ms=rtt_ms,
                status=status,
                kill_switch_active=state.kill_switch_active,
                kill_switch_source=getattr(state, "kill_switch_source", ""),
                binance_feed_age_ms=binance_age_ms,
                loop_lag_ms=loop_lag_ms,
                rolling_avg_ms=rolling_avg,
            )

            # Adaptive anomaly detection
            has_baseline = len(rtt_window) >= _MIN_SAMPLES_FOR_ADAPTIVE
            is_spike = (
                rtt_ms < 0
                or rtt_ms > _ABSOLUTE_WARN_MS
                or (has_baseline and rtt_ms > rolling_avg * _SPIKE_MULTIPLIER)
            )

            if rtt_ms < 0:
                logger.warning(
                    "PING FAIL (#%d consecutive) | avg=%.0fms"
                    " binance_age=%.0fms kill_switch=%s",
                    consecutive_fails, rolling_avg, binance_age_ms, state.kill_switch_active,
                )
            elif is_spike:
                logger.warning(
                    "PING SPIKE: %.0fms (60s avg=%.0fms, %.1f×) |"
                    " binance_age=%.0fms kill_switch=%s",
                    rtt_ms, rolling_avg, rtt_ms / max(rolling_avg, 1),
                    binance_age_ms, state.kill_switch_active,
                )

            # Correlate kill switch with network health using ADAPTIVE threshold
            if state.kill_switch_active and getattr(state, "kill_switch_source", "") == "binance":
                if is_spike or rtt_ms < 0:
                    logger.critical(
                        "DIAGNOSIS: Kill switch fired AND ping anomalous "
                        "(rtt=%.0fms, avg=%.0fms, %.1f×) "
                        "→ NETWORK is the culprit. Consider a VPS closer to Binance.",
                        rtt_ms, rolling_avg,
                        rtt_ms / max(rolling_avg, 1) if rtt_ms > 0 else 0,
                    )
                elif not has_baseline:
                    logger.critical(
                        "DIAGNOSIS: Kill switch fired. Ping=%.0fms but baseline not "
                        "yet established (%d samples). Cannot determine root cause yet.",
                        rtt_ms, len(rtt_window),
                    )
                else:
                    logger.critical(
                        "DIAGNOSIS: Kill switch fired but ping is NORMAL "
                        "(rtt=%.0fms, avg=%.0fms — only %.1f×). "
                        "Baseline RTT=%.0fms is high; this is a HIGH-LATENCY NETWORK. "
                        "Packet loss on WebSocket is likely — move to a VPS.",
                        rtt_ms, rolling_avg, rtt_ms / max(rolling_avg, 1), rolling_avg,
                    )

            # Sleep for the remainder of the interval
            elapsed = time.monotonic() - loop_start
            sleep_for = max(0.0, _INTERVAL_S - elapsed)
            await asyncio.sleep(sleep_for)


async def _ping(session: aiohttp.ClientSession) -> tuple[float, str]:
    """
    GET /api/v3/ping — Binance's lightest endpoint (returns {}).
    Returns (rtt_ms, status_string).
    rtt_ms = -1 on failure.
    """
    t0 = time.monotonic()
    try:
        async with session.get(
            _PING_URL,
            timeout=aiohttp.ClientTimeout(total=_TIMEOUT_S),
        ) as resp:
            await resp.read()   # consume body
            rtt_ms = (time.monotonic() - t0) * 1000
            if resp.status == 200:
                return rtt_ms, "ok"
            return rtt_ms, f"http_{resp.status}"
    except asyncio.TimeoutError:
        return -1.0, "timeout"
    except aiohttp.ClientError as exc:
        return -1.0, f"error:{type(exc).__name__}"
    except Exception as exc:
        return -1.0, f"error:{type(exc).__name__}"


def _ensure_header() -> None:
    if not os.path.exists(_CSV_PATH):
        with open(_CSV_PATH, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=_FIELDNAMES).writeheader()


def _write_row(
    rtt_ms: float,
    status: str,
    kill_switch_active: bool,
    kill_switch_source: str,
    binance_feed_age_ms: float,
    loop_lag_ms: float,
    rolling_avg_ms: float = 0.0,
) -> None:
    from datetime import datetime, timezone
    ts = datetime.now(tz=timezone.utc).isoformat()
    row = {
        "timestamp": ts,
        "rtt_ms": f"{rtt_ms:.1f}" if rtt_ms >= 0 else "FAIL",
        "status": status,
        "kill_switch_active": str(kill_switch_active),
        "kill_switch_source": kill_switch_source,
        "binance_feed_age_ms": f"{binance_feed_age_ms:.0f}",
        "loop_lag_ms": f"{loop_lag_ms:.1f}",
        "rolling_avg_ms": f"{rolling_avg_ms:.1f}",
    }
    try:
        with open(_CSV_PATH, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=_FIELDNAMES).writerow(row)
    except Exception as exc:
        logger.debug("ping_log write error: %s", exc)
