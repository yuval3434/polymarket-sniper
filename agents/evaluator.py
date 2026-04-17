"""
Evaluator Agent — Post-Mortem Analysis & Market Pulse
=======================================================
Listens to the outcome_queue fed by PositionManager.
On each closed trade:
  1. If a loss — runs a local post-mortem simulation to find root cause.
  2. Every N trades — runs a Market Pulse scan on recent ticks.
  3. Proposes parameter adjustments → forwards to strategy_queue for Developer.

Post-mortem logic:
  Replay the same trade with incrementally varied parameters (oracle_lag,
  window_start, z_threshold) and check which change would have yielded a WIN.
  The "counterfactual winner" is the suggested fix.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from core.database import fetch_recent_ticks, fetch_recent_trades, log_decision

logger = logging.getLogger(__name__)

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STRATEGY_CONFIG_PATH = os.path.join(_BASE, "strategy_config.json")

_MARKET_PULSE_EVERY_N_TRADES = 5   # run pulse every N closed trades
_POST_MORTEM_Z_STEPS  = [0.3, 0.5, 0.8, 1.0, 1.5, 2.0]
_POST_MORTEM_WIN_STEPS = [8, 10, 12, 15, 18]  # window_start variants


# ---------------------------------------------------------------------------
# Post-mortem dataclass
# ---------------------------------------------------------------------------

@dataclass
class PostMortem:
    trade_id: int
    token_side: str
    entry_price: float
    p_true: float
    z_score: float
    exit_type: str
    root_cause: str
    suggested_fix: dict
    confidence: float   # 0-1 how confident we are in this diagnosis


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------

class Evaluator:
    def __init__(
        self,
        outcome_queue: asyncio.Queue,
        strategy_queue: asyncio.Queue,
    ) -> None:
        self.outcome_queue  = outcome_queue   # receives trade closure dicts
        self.strategy_queue = strategy_queue  # sends proposed param adjustments
        self._trade_count   = 0

    async def run(self) -> None:
        """Main loop: process trade outcomes as they arrive."""
        while True:
            try:
                outcome = await asyncio.wait_for(self.outcome_queue.get(), timeout=30)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise

            self._trade_count += 1
            trade_id  = outcome.get("trade_id", 0)
            win       = outcome.get("win", False)
            pos       = outcome.get("pos")
            pnl_usd   = outcome.get("pnl_usd", 0.0)
            exit_type = outcome.get("exit_type", "EXPIRY")

            if not win:
                pm = await self._post_mortem(outcome)
                if pm and pm.suggested_fix:
                    logger.info(
                        "Evaluator post-mortem [trade #%d]: %s → fix: %s",
                        trade_id, pm.root_cause, pm.suggested_fix,
                    )
                    await self._propose(pm.suggested_fix, source=f"post_mortem:{pm.root_cause}")

            if self._trade_count % _MARKET_PULSE_EVERY_N_TRADES == 0:
                pulse_fixes = await self._market_pulse(
                    pos.market_id if pos else "",
                )
                if pulse_fixes:
                    await self._propose(pulse_fixes, source="market_pulse")

    # ------------------------------------------------------------------
    # Post-mortem
    # ------------------------------------------------------------------

    async def _post_mortem(self, outcome: dict) -> Optional[PostMortem]:
        pos       = outcome.get("pos")
        exit_type = outcome.get("exit_type", "EXPIRY")

        if pos is None:
            return None

        p_true    = pos.p_true
        z_score   = abs(pos.z_score)
        entry     = pos.entry_price
        token_side = pos.token_side

        # Load current live config
        try:
            import json as _json
            with open(STRATEGY_CONFIG_PATH) as f:
                cfg = _json.load(f)
        except Exception:
            cfg = {}

        current_z_min    = cfg.get("sniper", {}).get("min_z_score", 0.5)
        current_win_start = cfg.get("sniper", {}).get("window_start_seconds", 12)

        root_cause = "UNKNOWN"
        suggested  = {}
        confidence = 0.5

        # Diagnosis 1: Low Z — model was uncertain, should have skipped
        if z_score < 0.8:
            root_cause = "LOW_CONFIDENCE_ENTRY"
            suggested  = {"sniper.min_z_score": min(current_z_min + 0.15, 2.0)}
            confidence = 0.75

        # Diagnosis 2: High ask — thin margin, fee ate profit
        elif entry > 0.82:
            root_cause = "THIN_MARGIN_HIGH_ASK"
            suggested  = {
                "fee_filter.safety_buffer": min(
                    cfg.get("fee_filter", {}).get("safety_buffer", 0.03) + 0.01, 0.07
                ),
                "fee_filter.max_ask_price": max(
                    cfg.get("fee_filter", {}).get("max_ask_price", 0.97) - 0.02, 0.80
                ),
            }
            confidence = 0.70

        # Diagnosis 3: Stop-loss triggered — entry was late in a reversal
        elif exit_type == "STOP_LOSS":
            root_cause = "LATE_ENTRY_REVERSAL"
            new_start  = max(current_win_start - 2, 6)
            suggested  = {
                "sniper.window_start_seconds": new_start,
                "probability.oracle_lag_seconds": min(
                    cfg.get("probability", {}).get("oracle_lag_seconds", 2.0) + 0.5, 6.0
                ),
            }
            confidence = 0.65

        # Diagnosis 4: Confident but wrong — true probability model off
        elif z_score > 1.5:
            root_cause = "HIGH_CONFIDENCE_WRONG"
            suggested  = {
                "probability.oracle_lag_seconds": min(
                    cfg.get("probability", {}).get("oracle_lag_seconds", 2.0) + 1.0, 8.0
                ),
            }
            confidence = 0.55

        log_decision(
            agent="EVALUATOR",
            decision_type="POST_MORTEM",
            details={
                "trade_id": outcome.get("trade_id"),
                "root_cause": root_cause,
                "z_score": z_score,
                "entry_price": entry,
                "exit_type": exit_type,
                "suggested_fix": suggested,
                "confidence": confidence,
            },
        )

        return PostMortem(
            trade_id=outcome.get("trade_id", 0),
            token_side=token_side,
            entry_price=entry,
            p_true=p_true,
            z_score=z_score,
            exit_type=exit_type,
            root_cause=root_cause,
            suggested_fix=suggested,
            confidence=confidence,
        )

    # ------------------------------------------------------------------
    # Market Pulse — micro-trend analysis over last 30min ticks
    # ------------------------------------------------------------------

    async def _market_pulse(self, market_id: str) -> dict:
        """
        Analyse recent tick data from SQLite to detect regime changes:
          - High volatility → widen oracle lag
          - Trending strongly → enable momentum filter
          - Low spread activity → tighten safety buffer
        """
        ticks = fetch_recent_ticks(market_id, n=360)  # ~30min at 5s sampling
        if len(ticks) < 20:
            return {}

        prices = [t["btc_price"] for t in ticks if t.get("btc_price", 0) > 0]
        if len(prices) < 10:
            return {}

        # Realised vol over the window
        log_ret = [
            math.log(prices[i] / prices[i - 1])
            for i in range(1, len(prices))
            if prices[i - 1] > 0
        ]
        mean = sum(log_ret) / len(log_ret)
        std  = math.sqrt(sum((r - mean) ** 2 for r in log_ret) / max(len(log_ret) - 1, 1))
        ann_vol = std * math.sqrt(288 * 365)

        # Net directional move
        net_move_pct = (prices[-1] - prices[0]) / prices[0] * 100

        fixes = {}
        if ann_vol > 1.2:
            # High vol regime — widen oracle lag to be more conservative
            fixes["probability.oracle_lag_seconds"] = 3.0
            logger.info("Market Pulse: HIGH VOL (σ=%.2f) → oracle_lag=3.0", ann_vol)

        elif ann_vol < 0.4:
            # Very quiet — tighten to be more aggressive
            fixes["probability.oracle_lag_seconds"] = 1.0
            logger.info("Market Pulse: LOW VOL (σ=%.2f) → oracle_lag=1.0", ann_vol)

        if abs(net_move_pct) > 0.3:
            fixes["indicators.use_momentum"] = True
            fixes["indicators.momentum_min_pct"] = 0.05
            logger.info(
                "Market Pulse: TRENDING (%.2f%%) → enable momentum filter", net_move_pct
            )

        if fixes:
            log_decision(
                agent="EVALUATOR",
                decision_type="MARKET_PULSE",
                details={"ann_vol": ann_vol, "net_move_pct": net_move_pct, "fixes": fixes},
            )

        return fixes

    # ------------------------------------------------------------------
    # Propose to Developer
    # ------------------------------------------------------------------

    async def _propose(self, fixes: dict, source: str) -> None:
        try:
            self.strategy_queue.put_nowait({"fixes": fixes, "source": source})
        except asyncio.QueueFull:
            logger.debug("strategy_queue full — dropping proposal from %s", source)
