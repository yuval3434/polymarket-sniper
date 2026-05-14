"""
Researcher — Autonomous Strategy Research & Backtesting
========================================================
Generates candidate strategy configurations by mutating the current live
config based on Reviewer patterns, then validates them against historical
Binance kline data before passing to the Developer.

Backtesting model:
  - Fetch N 5-minute BTC/USDT klines from Binance REST.
  - Each candle: open = strike price, close = resolution price.
  - Simulate the sniper at T-12s using close price as proxy for spot price.
  - Apply candidate parameters and score by: win_rate, trades_taken, total_pnl.
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import random
from dataclasses import dataclass
from typing import Any

import aiohttp

from learning_agent.reviewer import AnalysisReport, LossPattern

logger = logging.getLogger(__name__)

_KLINES_URL = (
    "https://api.binance.com/api/v3/klines"
    "?symbol=BTCUSDT&interval=5m&limit=500"
)

# Parameter search bounds
_PARAM_BOUNDS: dict[str, tuple[float, float]] = {
    "probability.oracle_lag_seconds":       (0.5,  8.0),
    "probability.min_vol_floor":            (0.005, 0.05),
    "sniper.window_start_seconds":          (6,    20),
    "sniper.min_z_score":                   (0.2,  3.0),
    "fee_filter.safety_buffer":             (0.01, 0.08),
    "fee_filter.max_ask_price":             (0.80, 0.97),
    "fee_filter.maker_spread_threshold":    (0.001, 0.01),
    "indicators.momentum_min_pct":          (0.01, 0.10),
}

_MIN_BACKTEST_TRADES = 15   # reject candidate if it fires fewer than this many trades
_MIN_WIN_RATE = 0.52        # reject candidate with win rate below this


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int


@dataclass
class BacktestResult:
    params: dict
    trades_taken: int
    wins: int
    losses: int
    win_rate: float
    total_pnl: float
    score: float        # composite score used for ranking


# ---------------------------------------------------------------------------
# Kline fetcher
# ---------------------------------------------------------------------------

async def fetch_klines(limit: int = 500) -> list[Candle]:
    url = f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=5m&limit={limit}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.error("Binance klines returned %s", resp.status)
                    return []
                raw = await resp.json()
                return [
                    Candle(
                        open_time=int(c[0]),
                        open=float(c[1]),
                        high=float(c[2]),
                        low=float(c[3]),
                        close=float(c[4]),
                        volume=float(c[5]),
                        close_time=int(c[6]),
                    )
                    for c in raw
                ]
    except Exception as exc:
        logger.error("Failed to fetch klines: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Core backtest simulation
# ---------------------------------------------------------------------------

def _simulate_vol(candles: list[Candle], idx: int, lookback: int = 6) -> float:
    """Approximate 30-minute realized vol from recent candles."""
    start = max(0, idx - lookback)
    log_returns = [
        math.log(candles[i].close / candles[i - 1].close)
        for i in range(start + 1, idx + 1)
        if candles[i - 1].close > 0
    ]
    if len(log_returns) < 2:
        return 0.02
    mean = sum(log_returns) / len(log_returns)
    var = sum((r - mean) ** 2 for r in log_returns) / max(len(log_returns) - 1, 1)
    std = math.sqrt(var)
    # Annualize: 288 five-min candles per day
    return std * math.sqrt(288 * 365)


def _simulate_momentum(candles: list[Candle], idx: int) -> float:
    """Price change % over last 2 candles as a momentum proxy."""
    if idx < 2:
        return 0.0
    return (candles[idx].close - candles[idx - 2].close) / candles[idx - 2].close * 100


def backtest(params: dict, candles: list[Candle]) -> BacktestResult:
    """
    Simulate the sniper strategy on historical candles using candidate params.

    For each candle (treating open as strike, close as resolution):
      1. Compute P_up / P_down via log-normal model.
      2. Apply fee filter.
      3. Apply optional momentum filter.
      4. Record WIN if direction was correct, LOSS otherwise.
    """
    from scipy.stats import norm  # type: ignore

    oracle_lag = params["probability"]["oracle_lag_seconds"]
    min_vol = params["probability"]["min_vol_floor"]
    min_z = params["sniper"]["min_z_score"]
    safety_buf = params["fee_filter"]["safety_buffer"]
    max_ask = params["fee_filter"]["max_ask_price"]
    use_momentum = params["indicators"]["use_momentum"]
    momentum_min = params["indicators"]["momentum_min_pct"]

    # V2: fee rate is market-specific and fetched live via get_clob_market_info().
    # Offline backtests have no live CLOB client, so use 0.0 (no-fee assumption).
    # This makes backtests slightly optimistic; factor in ~1-2% when interpreting results.
    fee_rate = 0.0

    trades = 0
    wins = 0
    losses = 0
    total_pnl = 0.0

    # Simulate: entry at T-12s ≈ using close price as the "spot price at snipe time"
    tau = 12.0 + oracle_lag
    tau_years = tau / (365 * 24 * 3600)

    for i in range(6, len(candles) - 1):  # skip first 6 for vol warmup; skip last (incomplete)
        candle = candles[i]
        S = candle.close      # BTC spot at T-12s (proxy)
        K = candle.open       # strike = candle open

        if S <= 0 or K <= 0:
            continue

        sigma = max(_simulate_vol(candles, i), min_vol)
        sigma_tau = sigma * math.sqrt(tau_years)
        if sigma_tau <= 0:
            continue

        Z = math.log(S / K) / sigma_tau
        abs_Z = abs(Z)

        # Z threshold filter
        if abs_Z < min_z:
            continue

        # Choose side
        if Z > 0:
            side = "UP"
            # Simulate market ask as a function of Z (higher Z → higher ask)
            ask = min(0.50 + abs_Z * 0.08, 0.95)
        else:
            side = "DOWN"
            ask = min(0.50 + abs_Z * 0.08, 0.95)

        # Fee filter
        if ask >= max_ask:
            continue
        fee = fee_rate * ask
        ev = (float(norm.cdf(abs_Z))) * (1.0 - ask - fee) + (1 - float(norm.cdf(abs_Z))) * (-(ask + fee))
        gross = 1.0 - ask * (1 + fee_rate)
        if ev <= safety_buf or gross <= safety_buf:
            continue

        # Momentum filter (optional)
        if use_momentum:
            mom = _simulate_momentum(candles, i)
            if abs(mom) < momentum_min:
                continue
            # Check direction agreement: positive momentum → expect UP
            if mom > 0 and side == "DOWN":
                continue
            if mom < 0 and side == "UP":
                continue

        # Resolution: next candle's close vs current candle open
        next_close = candles[i + 1].close
        resolved_up = next_close >= K
        won = (side == "UP" and resolved_up) or (side == "DOWN" and not resolved_up)

        trades += 1
        pnl = (1.0 - ask - fee) if won else -(ask + fee)
        total_pnl += pnl
        if won:
            wins += 1
        else:
            losses += 1

    win_rate = wins / trades if trades > 0 else 0.0
    # Score: blend win_rate and total_pnl, penalise very low trade count
    trade_penalty = 1.0 if trades >= _MIN_BACKTEST_TRADES else 0.0
    score = (win_rate * 0.6 + min(total_pnl / max(trades, 1) * 5, 0.4)) * trade_penalty

    return BacktestResult(
        params=params,
        trades_taken=trades,
        wins=wins,
        losses=losses,
        win_rate=win_rate,
        total_pnl=total_pnl,
        score=score,
    )


# ---------------------------------------------------------------------------
# Candidate generation
# ---------------------------------------------------------------------------

def _clamp(key: str, value: float) -> float:
    lo, hi = _PARAM_BOUNDS.get(key, (value, value))
    return max(lo, min(hi, value))


def _get_nested(d: dict, dotkey: str) -> Any:
    keys = dotkey.split(".")
    v = d
    for k in keys:
        v = v[k]
    return v


def _set_nested(d: dict, dotkey: str, value: Any) -> None:
    keys = dotkey.split(".")
    v = d
    for k in keys[:-1]:
        v = v[k]
    v[keys[-1]] = value


def _apply_pattern_hints(base: dict, patterns: list[LossPattern]) -> dict:
    """Apply Reviewer-suggested parameter deltas from detected patterns."""
    candidate = copy.deepcopy(base)
    for pattern in patterns:
        for dotkey, delta in pattern.suggested_fix.items():
            try:
                if isinstance(delta, bool):
                    _set_nested(candidate, dotkey, delta)
                else:
                    current = _get_nested(candidate, dotkey)
                    new_val = _clamp(dotkey, float(current) + float(delta))
                    _set_nested(candidate, dotkey, new_val)
            except (KeyError, TypeError):
                pass
    return candidate


def _random_perturbation(base: dict, scale: float = 0.15) -> dict:
    """Randomly perturb numeric parameters within ±scale fraction of their range."""
    candidate = copy.deepcopy(base)
    for dotkey, (lo, hi) in _PARAM_BOUNDS.items():
        try:
            current = float(_get_nested(candidate, dotkey))
            span = (hi - lo) * scale
            new_val = _clamp(dotkey, current + random.uniform(-span, span))
            # Preserve int type for integer params
            if isinstance(_get_nested(candidate, dotkey), int):
                new_val = int(round(new_val))
            _set_nested(candidate, dotkey, new_val)
        except (KeyError, TypeError, ValueError):
            pass
    return candidate


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def research(
    live_config: dict,
    report: AnalysisReport,
    n_random: int = 8,
) -> list[BacktestResult]:
    """
    Generate and backtest candidate configurations.
    Returns results sorted by score (best first).

    Strategy:
      1. One candidate derived from Reviewer pattern hints.
      2. n_random randomly perturbed candidates.
    """
    candles = await fetch_klines(500)
    if len(candles) < 20:
        logger.error("Insufficient kline data for backtesting (%d candles)", len(candles))
        return []

    candidates: list[dict] = []

    # Pattern-driven candidate
    if report.patterns:
        candidates.append(_apply_pattern_hints(live_config, report.patterns))
        logger.info("Researcher: added 1 pattern-driven candidate")

    # Random exploration candidates
    for _ in range(n_random):
        candidates.append(_random_perturbation(live_config))

    # Always include the current live config as baseline comparison
    candidates.append(copy.deepcopy(live_config))

    # Run all backtests in a thread so the event loop stays responsive.
    # Each backtest is pure-Python CPU work (~50-200ms); moving to a thread
    # releases the event loop between GIL switches even though there is no
    # true parallelism.
    def _run_backtests() -> list:
        return sorted(
            [backtest(c, candles) for c in candidates],
            key=lambda r: r.score, reverse=True,
        )
    results = await asyncio.to_thread(_run_backtests)

    for i, r in enumerate(results[:3]):
        logger.info(
            "Researcher rank #%d: trades=%d win_rate=%.1f%% pnl=%.4f score=%.4f",
            i + 1, r.trades_taken, r.win_rate, r.total_pnl, r.score,
        )

    # Filter: only return candidates that beat minimum viability thresholds
    viable = [
        r for r in results
        if r.trades_taken >= _MIN_BACKTEST_TRADES and r.win_rate >= _MIN_WIN_RATE
    ]
    logger.info(
        "Researcher: %d / %d candidates passed viability threshold",
        len(viable), len(results),
    )
    return viable
