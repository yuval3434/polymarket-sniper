"""
Shadow Trade Analyser
=====================
Analyses the shadow_trades table to find missed profitable opportunities.

For each rejection reason (z_too_low, price_too_high, ev_negative) it checks:
  - How many skipped trades would have been profitable?
  - What was the average theoretical PnL?

If a reason has win_rate > WIN_RATE_THRESHOLD with enough samples, it logs
a concrete suggestion to lower the relevant parameter and writes it to
learning_history.json for the developer agent to pick up.
"""
from __future__ import annotations

import json
import logging
import os
import time

logger = logging.getLogger(__name__)

WIN_RATE_THRESHOLD = 0.60   # 60% win rate on skipped trades → suggest relaxing
MIN_SAMPLES        = 5      # minimum resolved shadow trades before drawing conclusions

_BASE         = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_HISTORY_FILE = os.path.join(_BASE, "learning_history.json")


def analyse_shadow_trades() -> list[dict]:
    """
    Pull shadow trade stats from DB and return a list of suggestion dicts.
    Safe to call from asyncio.to_thread.
    """
    from core.database import fetch_shadow_trade_stats
    stats = fetch_shadow_trade_stats(min_count=MIN_SAMPLES)

    suggestions: list[dict] = []
    for row in stats:
        reason   = row["rejection_reason"]
        total    = row["total"]
        wins     = row["wins"]
        avg_pnl  = row["avg_pnl"]
        avg_z    = row["avg_z"]
        avg_p    = row["avg_p_true"]
        win_rate = wins / total if total else 0.0

        logger.info(
            "[SHADOW] %s: %d skipped trades, %.0f%% would have won, avg PnL=$%.3f  avg_Z=%.3f",
            reason, total, win_rate * 100, avg_pnl, avg_z,
        )

        if win_rate < WIN_RATE_THRESHOLD:
            logger.info("[SHADOW] %s: win rate %.0f%% below threshold — no change suggested", reason, win_rate * 100)
            continue

        suggestion = _build_suggestion(reason, win_rate, total, avg_pnl, avg_z, avg_p)
        if suggestion:
            suggestions.append(suggestion)
            logger.warning(
                "[SHADOW] SUGGESTION — %s: skipped %d trades (%.0f%% profitable, avg PnL=$%.3f). "
                "Recommend: %s",
                reason, total, win_rate * 100, avg_pnl, suggestion["action"],
            )

    if suggestions:
        _append_to_history(suggestions)

    return suggestions


def _build_suggestion(
    reason: str,
    win_rate: float,
    total: int,
    avg_pnl: float,
    avg_z: float,
    avg_p: float,
) -> dict | None:
    import engine.strategy_loader as _scfg

    if reason == "z_too_low":
        current = _scfg.min_z_score()
        # Suggest lowering by ~10% of current value, floor at 0.10
        new_val = round(max(current * 0.90, 0.10), 3)
        if new_val >= current:
            return None
        return {
            "reason": reason,
            "action": f"Lower min_z_score {current:.3f} → {new_val:.3f}",
            "param":  "sniper.min_z_score",
            "current_value": current,
            "suggested_value": new_val,
            "evidence": {"skipped": total, "win_rate": round(win_rate, 3), "avg_pnl": avg_pnl, "avg_z": avg_z},
        }

    if reason == "price_too_high":
        current = _scfg.max_ask_price()
        new_val = round(min(current + 0.05, 0.95), 2)
        if new_val <= current:
            return None
        return {
            "reason": reason,
            "action": f"Raise max_ask_price {current:.2f} → {new_val:.2f}",
            "param":  "fee_filter.max_ask_price",
            "current_value": current,
            "suggested_value": new_val,
            "evidence": {"skipped": total, "win_rate": round(win_rate, 3), "avg_pnl": avg_pnl},
        }

    if reason == "ev_negative":
        current = _scfg.safety_buffer()
        new_val = round(max(current - 0.005, 0.005), 4)
        if new_val >= current:
            return None
        return {
            "reason": reason,
            "action": f"Lower safety_buffer {current:.4f} → {new_val:.4f}",
            "param":  "fee_filter.safety_buffer",
            "current_value": current,
            "suggested_value": new_val,
            "evidence": {"skipped": total, "win_rate": round(win_rate, 3), "avg_pnl": avg_pnl},
        }

    return None


def _append_to_history(suggestions: list[dict]) -> None:
    try:
        history: list = []
        if os.path.exists(_HISTORY_FILE):
            with open(_HISTORY_FILE) as f:
                history = json.load(f)
        for s in suggestions:
            history.append({
                "event":     "shadow_trade_suggestion",
                "ts":        time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "reason":    s["reason"],
                "action":    s["action"],
                "evidence":  s.get("evidence", {}),
            })
        with open(_HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=2)
    except Exception as exc:
        logger.warning("[SHADOW] Could not write to learning_history.json: %s", exc)
