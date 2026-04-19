"""
Loop Lag Sensor — Event Loop Blocking Detector
================================================
Schedules a coroutine to wake up every PROBE_INTERVAL_S.
If it actually wakes up late by > WARN_THRESHOLD_MS, the event loop
was blocked by a synchronous operation during that window.

This is the definitive proof of event-loop blocking:
  - loop_lag high + ping healthy  →  blocking code (SQLite, file I/O, CPU)
  - loop_lag high + ping degraded →  network caused the event loop to stall
    (e.g. aiohttp waiting on a dead socket and not yielding)

Logs a WARNING with the lag amount every time the threshold is crossed.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque

logger = logging.getLogger(__name__)

PROBE_INTERVAL_S  = 0.5     # wake up every 500ms
WARN_THRESHOLD_MS = 200.0   # log warning if we slept > 200ms longer than expected
CRIT_THRESHOLD_MS = 1000.0  # log critical if we slept > 1s longer than expected
_ROLLING_WINDOW   = 120     # keep last 120 samples (60 seconds at 500ms)


async def loop_lag_sensor() -> None:
    """
    Runs forever. On each wake-up computes:
        actual_sleep = now - last_wake
        lag          = actual_sleep - PROBE_INTERVAL_S

    A lag > WARN_THRESHOLD_MS means something blocked the event loop.
    """
    window: deque[float] = deque(maxlen=_ROLLING_WINDOW)
    last_wake = time.monotonic()
    report_counter = 0

    while True:
        await asyncio.sleep(PROBE_INTERVAL_S)
        now = time.monotonic()
        actual_ms = (now - last_wake) * 1000
        lag_ms = actual_ms - (PROBE_INTERVAL_S * 1000)
        last_wake = now

        window.append(lag_ms)
        report_counter += 1

        if lag_ms >= CRIT_THRESHOLD_MS:
            logger.critical(
                "EVENT LOOP BLOCKED for %.0fms  (expected %.0fms, got %.0fms)"
                " — this is what causes 'Binance feed stale' errors.",
                lag_ms, PROBE_INTERVAL_S * 1000, actual_ms,
            )
        elif lag_ms >= WARN_THRESHOLD_MS:
            logger.warning(
                "Loop lag: %.0fms  (event loop was blocked — find the sync call)",
                lag_ms,
            )

        # Print a rolling summary every 60 seconds
        if report_counter % 120 == 0:
            avg_lag = sum(window) / len(window)
            max_lag = max(window)
            p95_lag = sorted(window)[int(len(window) * 0.95)]
            over_200 = sum(1 for x in window if x >= WARN_THRESHOLD_MS)
            logger.info(
                "Loop lag 60s summary: avg=%.0fms  p95=%.0fms  max=%.0fms"
                "  spikes(>200ms)=%d/%d",
                avg_lag, p95_lag, max_lag, over_200, len(window),
            )
