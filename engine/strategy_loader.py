"""
Strategy Loader — Live config reader for engine modules
=========================================================
Reads strategy_config.json and caches it with a short TTL so the engine
picks up promotions (new live config) without requiring a bot restart.

Both engine modules import from here instead of hardcoding values.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

_BASE        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CONFIG_PATH = os.path.join(_BASE, "strategy_config.json")
_CACHE_TTL   = 10.0   # re-read file at most every 10 seconds

_cache: dict        = {}
_cache_ts: float    = 0.0


def _load() -> dict:
    global _cache, _cache_ts
    now = time.monotonic()
    if now - _cache_ts < _CACHE_TTL and _cache:
        return _cache
    try:
        with open(_CONFIG_PATH) as f:
            _cache    = json.load(f)
            _cache_ts = now
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        if not _cache:
            logger.warning("strategy_config.json unreadable (%s) — using defaults", exc)
            _cache = _defaults()
    return _cache


def _defaults() -> dict:
    return {
        "probability": {"oracle_lag_seconds": 2.0, "min_vol_floor": 0.01},
        "sniper": {
            "window_open_seconds":  290,
            "window_close_seconds": 90,
            "window_stop_seconds":  2,
            "early_cutoff_seconds": 180,
            "early_z_score":        0.3,
            "min_z_score":          0.5,
        },
        "fee_filter":  {"safety_buffer": 0.03, "max_ask_price": 0.75, "maker_spread_threshold": 0.002},
        "indicators":  {
            "use_momentum": False, "momentum_lookback_seconds": 30,
            "momentum_min_pct": 0.05, "use_spread_filter": False, "max_poly_spread": 0.10,
        },
    }


# ---------------------------------------------------------------------------
# Typed accessors used by engine modules
# ---------------------------------------------------------------------------

def oracle_lag_seconds() -> float:
    return float(_load().get("probability", {}).get("oracle_lag_seconds", 2.0))

def min_vol_floor() -> float:
    return float(_load().get("probability", {}).get("min_vol_floor", 0.01))

def safety_buffer() -> float:
    return float(_load().get("fee_filter", {}).get("safety_buffer", 0.03))

def max_ask_price() -> float:
    return float(_load().get("fee_filter", {}).get("max_ask_price", 0.97))

def maker_spread_threshold() -> float:
    return float(_load().get("fee_filter", {}).get("maker_spread_threshold", 0.002))

def min_z_score() -> float:
    return float(_load().get("sniper", {}).get("min_z_score", 0.5))

def early_z_score() -> float:
    return float(_load().get("sniper", {}).get("early_z_score", 0.3))

def window_open_seconds() -> int:
    return int(_load().get("sniper", {}).get("window_open_seconds", 290))

def window_close_seconds() -> int:
    return int(_load().get("sniper", {}).get("window_close_seconds", 90))

def early_cutoff_seconds() -> int:
    return int(_load().get("sniper", {}).get("early_cutoff_seconds", 180))

def window_stop_seconds() -> int:
    return int(_load().get("sniper", {}).get("window_stop_seconds", 2))

def window_start_seconds() -> int:
    """Backward-compat alias for window_open_seconds."""
    return window_open_seconds()

def use_momentum() -> bool:
    return bool(_load().get("indicators", {}).get("use_momentum", False))

def momentum_min_pct() -> float:
    return float(_load().get("indicators", {}).get("momentum_min_pct", 0.05))

def use_spread_filter() -> bool:
    return bool(_load().get("indicators", {}).get("use_spread_filter", False))

def max_poly_spread() -> float:
    return float(_load().get("indicators", {}).get("max_poly_spread", 0.10))

def strategy_version() -> int:
    return int(_load().get("_meta", {}).get("version", 1))

def take_profit_pct() -> float:
    return float(_load().get("exit", {}).get("take_profit_pct", 0.05))

def take_profit_min_tau() -> float:
    return float(_load().get("exit", {}).get("take_profit_min_tau", 30.0))

def hte_p_threshold() -> float:
    return float(_load().get("exit", {}).get("hte_p_threshold", 0.98))

def hte_tau_threshold() -> float:
    return float(_load().get("exit", {}).get("hte_tau_threshold", 15.0))

def emergency_btc_move_pct() -> float:
    return float(_load().get("exit", {}).get("emergency_btc_move_pct", 0.0005))

def emergency_btc_window_s() -> float:
    return float(_load().get("exit", {}).get("emergency_btc_window_s", 1.0))

def max_gas_claim_pct() -> float:
    return float(_load().get("exit", {}).get("max_gas_claim_pct", 0.02))

def strategy_name() -> str:
    """Human-readable strategy label for the tracker/dashboard."""
    meta    = _load().get("_meta", {})
    version = meta.get("version", 1)
    exp_id  = meta.get("promoted_from_experiment") or meta.get("experiment_id", "")
    if meta.get("baseline"):
        return f"v{version}-baseline"
    if exp_id:
        return f"v{version}-{str(exp_id)[-8:]}"
    return f"v{version}"
