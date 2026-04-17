"""
Reviewer — Data Ingestion & Error Analysis
==========================================
Parses paper_trades.csv, classifies loss patterns, and returns a
structured AnalysisReport that the Researcher uses to form hypotheses.
"""
from __future__ import annotations

import csv
import logging
import math
import os
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

PAPER_TRADES_CSV = os.path.join(os.path.dirname(__file__), "..", "paper_trades.csv")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Trade:
    timestamp: str
    market_id: str
    btc_price: float
    opening_price: float
    token_side: str         # "UP" or "DOWN"
    ask_price: float
    p_true: float
    z_score: float
    expected_value: float
    would_execute: bool
    fee_adjusted_profit: float
    actual_outcome: str     # "WIN", "LOSS", "PENDING"
    actual_pnl: float


@dataclass
class LossPattern:
    name: str
    count: int
    description: str
    suggested_fix: dict     # parameter delta suggestion


@dataclass
class AnalysisReport:
    total_trades: int
    executed_trades: int
    wins: int
    losses: int
    pending: int
    win_rate: float
    avg_pnl: float
    avg_z_score_wins: float
    avg_z_score_losses: float
    avg_ask_wins: float
    avg_ask_losses: float
    patterns: list[LossPattern] = field(default_factory=list)
    raw_trades: list[Trade] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"Trades: {self.executed_trades} executed / {self.total_trades} evaluated",
            f"Outcomes: {self.wins}W / {self.losses}L / {self.pending} pending",
            f"Win rate: {self.win_rate:.1%}  |  Avg PnL: {self.avg_pnl:+.4f}",
            f"Avg |Z| wins={self.avg_z_score_wins:.3f}  losses={self.avg_z_score_losses:.3f}",
            f"Avg ask  wins={self.avg_ask_wins:.4f}  losses={self.avg_ask_losses:.4f}",
        ]
        for p in self.patterns:
            lines.append(f"  PATTERN [{p.name}] x{p.count}: {p.description}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------

def _load_trades(csv_path: str) -> list[Trade]:
    trades: list[Trade] = []
    if not os.path.exists(csv_path):
        return trades
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            try:
                trades.append(Trade(
                    timestamp=row.get("timestamp", ""),
                    market_id=row.get("market_id", ""),
                    btc_price=float(row.get("btc_price", 0)),
                    opening_price=float(row.get("opening_price", 0)),
                    token_side=row.get("token_side", ""),
                    ask_price=float(row.get("ask_price", 0)),
                    p_true=float(row.get("P_true", 0.5)),
                    z_score=float(row.get("Z_score", 0)),
                    expected_value=float(row.get("expected_value", 0)),
                    would_execute=row.get("would_execute", "False").lower() == "true",
                    fee_adjusted_profit=float(row.get("fee_adjusted_profit_if_right", 0)),
                    actual_outcome=row.get("actual_outcome", "PENDING"),
                    actual_pnl=float(row.get("actual_pnl") or 0),
                ))
            except (ValueError, KeyError):
                continue
    return trades


# ---------------------------------------------------------------------------
# Pattern detectors
# ---------------------------------------------------------------------------

def _detect_low_z_losses(losses: list[Trade]) -> Optional[LossPattern]:
    """Losses where |Z| was low (low confidence) — Z threshold too permissive."""
    low_z = [t for t in losses if abs(t.z_score) < 0.8]
    if len(low_z) >= 3:
        return LossPattern(
            name="LOW_Z_LOSSES",
            count=len(low_z),
            description=f"{len(low_z)} losses had |Z|<0.8 — model had low confidence",
            suggested_fix={"sniper.min_z_score": +0.2},
        )
    return None


def _detect_high_ask_losses(losses: list[Trade]) -> Optional[LossPattern]:
    """Losses where ask was high — overpaying for shares with thin margin."""
    high_ask = [t for t in losses if t.ask_price > 0.80]
    if len(high_ask) >= 3:
        return LossPattern(
            name="HIGH_ASK_LOSSES",
            count=len(high_ask),
            description=f"{len(high_ask)} losses had ask>0.80 — thin margin, tightened buffer needed",
            suggested_fix={"fee_filter.safety_buffer": +0.01, "fee_filter.max_ask_price": -0.03},
        )
    return None


def _detect_wrong_direction(losses: list[Trade], wins: list[Trade]) -> Optional[LossPattern]:
    """
    Loss rate is higher on one side (UP vs DOWN).
    Suggests a directional bias that momentum filtering could correct.
    """
    loss_up = sum(1 for t in losses if t.token_side == "UP")
    loss_down = sum(1 for t in losses if t.token_side == "DOWN")
    if not losses:
        return None
    bias_ratio = max(loss_up, loss_down) / len(losses)
    if bias_ratio > 0.70 and len(losses) >= 4:
        bias_side = "UP" if loss_up > loss_down else "DOWN"
        return LossPattern(
            name="DIRECTIONAL_BIAS",
            count=len(losses),
            description=f"{bias_ratio:.0%} of losses were {bias_side} trades — momentum filter may help",
            suggested_fix={"indicators.use_momentum": True, "indicators.momentum_min_pct": 0.03},
        )
    return None


def _detect_late_entry(losses: list[Trade]) -> Optional[LossPattern]:
    """
    If mean Z of losses is not much lower than wins, the model is fine but
    entries may be timed poorly (window too wide — catching mean-reversion).
    Tighten window_start.
    """
    # Proxy: losses with Z > 1.5 suggest the model was confident but still lost.
    # This implies late entry / price reversal in the final seconds.
    confident_losses = [t for t in losses if abs(t.z_score) > 1.5]
    if len(confident_losses) >= 3:
        return LossPattern(
            name="CONFIDENT_LOSSES",
            count=len(confident_losses),
            description=f"{len(confident_losses)} high-confidence losses (|Z|>1.5) — possible late reversal",
            suggested_fix={"sniper.window_start_seconds": -2, "probability.oracle_lag_seconds": +0.5},
        )
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyse(csv_path: str = PAPER_TRADES_CSV) -> AnalysisReport:
    """Load trades and return a full AnalysisReport."""
    all_trades = _load_trades(csv_path)
    executed = [t for t in all_trades if t.would_execute]
    wins = [t for t in executed if t.actual_outcome == "WIN"]
    losses = [t for t in executed if t.actual_outcome == "LOSS"]
    pending = [t for t in executed if t.actual_outcome == "PENDING"]

    def safe_mean(vals: list[float]) -> float:
        return sum(vals) / len(vals) if vals else 0.0

    win_rate = len(wins) / len(executed) if executed else 0.0

    patterns: list[LossPattern] = []
    for detector in (
        _detect_low_z_losses,
        _detect_high_ask_losses,
        _detect_late_entry,
    ):
        p = detector(losses)
        if p:
            patterns.append(p)

    dir_pattern = _detect_wrong_direction(losses, wins)
    if dir_pattern:
        patterns.append(dir_pattern)

    report = AnalysisReport(
        total_trades=len(all_trades),
        executed_trades=len(executed),
        wins=len(wins),
        losses=len(losses),
        pending=len(pending),
        win_rate=win_rate,
        avg_pnl=safe_mean([t.actual_pnl for t in wins + losses]),
        avg_z_score_wins=safe_mean([abs(t.z_score) for t in wins]),
        avg_z_score_losses=safe_mean([abs(t.z_score) for t in losses]),
        avg_ask_wins=safe_mean([t.ask_price for t in wins]),
        avg_ask_losses=safe_mean([t.ask_price for t in losses]),
        patterns=patterns,
        raw_trades=executed,
    )

    logger.info("Reviewer:\n%s", report.summary())
    return report
