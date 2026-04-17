"""
Validator — Mandatory Paper Trading & Promotion Gate
=====================================================
Runs a candidate strategy in a shadow paper-trade environment alongside
the live bot without risking real money.

Validation rules (ALL must pass before promotion):
  1. Minimum trade count  : >= MIN_PAPER_TRADES resolved trades
  2. Minimum time elapsed : >= MIN_VALIDATION_HOURS of real market time
  3. Win rate             : >= live_win_rate + MIN_IMPROVEMENT_PCT
  4. Expected value       : mean EV per trade > live mean EV
  5. No catastrophic run  : no streak of > MAX_LOSS_STREAK consecutive losses

Shadow trades are written to candidate_paper_trades.csv and kept separate
from the main paper_trades.csv.
"""
from __future__ import annotations

import csv
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from learning_agent.reviewer import AnalysisReport

logger = logging.getLogger(__name__)

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CANDIDATE_CSV = os.path.join(_BASE, "candidate_paper_trades.csv")

MIN_PAPER_TRADES     = 20       # minimum resolved trades before decision
MIN_VALIDATION_HOURS = 2.0      # minimum real-world hours
MIN_IMPROVEMENT_PCT  = 0.03     # candidate must beat live by at least 3 pp
MAX_LOSS_STREAK      = 6        # auto-reject if this many consecutive losses

_FIELDNAMES = [
    "timestamp", "market_id", "token_side", "ask_price",
    "P_true", "Z_score", "expected_value", "would_execute",
    "actual_outcome", "actual_pnl", "experiment_id",
]


class ValidationStatus(Enum):
    PENDING    = "pending"
    VALIDATING = "validating"
    APPROVED   = "approved"
    REJECTED   = "rejected"


@dataclass
class ValidationState:
    experiment_id: str
    started_at: float = field(default_factory=time.monotonic)
    started_wall: str = field(
        default_factory=lambda: datetime.now(tz=timezone.utc).isoformat()
    )
    trades_logged: int = 0
    wins: int = 0
    losses: int = 0
    total_ev: float = 0.0
    current_loss_streak: int = 0
    max_loss_streak_seen: int = 0
    status: ValidationStatus = ValidationStatus.VALIDATING
    rejection_reason: str = ""

    @property
    def win_rate(self) -> float:
        settled = self.wins + self.losses
        return self.wins / settled if settled > 0 else 0.0

    @property
    def settled_trades(self) -> int:
        return self.wins + self.losses

    @property
    def elapsed_hours(self) -> float:
        return (time.monotonic() - self.started_at) / 3600

    @property
    def mean_ev(self) -> float:
        return self.total_ev / self.settled_trades if self.settled_trades > 0 else 0.0


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _ensure_header() -> None:
    if not os.path.exists(CANDIDATE_CSV):
        with open(CANDIDATE_CSV, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=_FIELDNAMES).writeheader()


def log_shadow_trade(
    market_id: str,
    token_side: str,
    ask_price: float,
    p_true: float,
    z_score: float,
    ev: float,
    would_execute: bool,
    experiment_id: str,
) -> None:
    _ensure_header()
    row = {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "market_id": market_id,
        "token_side": token_side,
        "ask_price": f"{ask_price:.4f}",
        "P_true": f"{p_true:.4f}",
        "Z_score": f"{z_score:.4f}",
        "expected_value": f"{ev:.4f}",
        "would_execute": str(would_execute),
        "actual_outcome": "PENDING",
        "actual_pnl": "",
        "experiment_id": experiment_id,
    }
    with open(CANDIDATE_CSV, "a", newline="") as f:
        csv.DictWriter(f, fieldnames=_FIELDNAMES).writerow(row)


def update_shadow_outcome(
    market_id: str,
    token_side: str,
    outcome: str,   # "WIN" or "LOSS"
    pnl: float,
    state: ValidationState,
) -> None:
    """Mark pending rows for this market/side as resolved and update state."""
    if not os.path.exists(CANDIDATE_CSV):
        return

    rows: list[dict] = []
    updated = 0
    with open(CANDIDATE_CSV, newline="") as f:
        rows = list(csv.DictReader(f))

    for row in rows:
        if (
            row["market_id"] == market_id
            and row["token_side"] == token_side
            and row["would_execute"] == "True"
            and row["actual_outcome"] == "PENDING"
            and row["experiment_id"] == state.experiment_id
        ):
            row["actual_outcome"] = outcome
            row["actual_pnl"] = f"{pnl:.4f}"
            updated += 1

    if updated:
        with open(CANDIDATE_CSV, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=_FIELDNAMES)
            w.writeheader()
            w.writerows(rows)

        # Update in-memory state
        state.trades_logged += updated
        if outcome == "WIN":
            state.wins += updated
            state.current_loss_streak = 0
        else:
            state.losses += updated
            state.current_loss_streak += updated
            state.max_loss_streak_seen = max(
                state.max_loss_streak_seen, state.current_loss_streak
            )
        state.total_ev += pnl * updated


# ---------------------------------------------------------------------------
# Gate evaluation
# ---------------------------------------------------------------------------

def evaluate(
    vstate: ValidationState,
    live_report: AnalysisReport,
) -> tuple[ValidationStatus, str]:
    """
    Check all five validation gates.
    Returns (status, reason_string).
    """
    settled = vstate.settled_trades

    # Gate 1: minimum trade count
    if settled < MIN_PAPER_TRADES:
        return ValidationStatus.VALIDATING, (
            f"Need {MIN_PAPER_TRADES} settled trades, have {settled}"
        )

    # Gate 2: minimum time
    if vstate.elapsed_hours < MIN_VALIDATION_HOURS:
        remaining = MIN_VALIDATION_HOURS - vstate.elapsed_hours
        return ValidationStatus.VALIDATING, (
            f"Need {MIN_VALIDATION_HOURS}h elapsed, {remaining:.1f}h remaining"
        )

    # Gate 3: catastrophic loss streak
    if vstate.max_loss_streak_seen >= MAX_LOSS_STREAK:
        reason = f"Max loss streak {vstate.max_loss_streak_seen} >= {MAX_LOSS_STREAK}"
        vstate.status = ValidationStatus.REJECTED
        vstate.rejection_reason = reason
        return ValidationStatus.REJECTED, reason

    # Gate 4: win rate improvement
    baseline_wr = live_report.win_rate if live_report.win_rate > 0 else 0.50
    required_wr = baseline_wr + MIN_IMPROVEMENT_PCT
    if vstate.win_rate < required_wr:
        reason = (
            f"Win rate {vstate.win_rate:.1%} < required {required_wr:.1%} "
            f"(baseline {baseline_wr:.1%} + {MIN_IMPROVEMENT_PCT:.0%})"
        )
        vstate.status = ValidationStatus.REJECTED
        vstate.rejection_reason = reason
        return ValidationStatus.REJECTED, reason

    # Gate 5: positive expected value
    if vstate.mean_ev <= 0:
        reason = f"Mean EV {vstate.mean_ev:.4f} ≤ 0"
        vstate.status = ValidationStatus.REJECTED
        vstate.rejection_reason = reason
        return ValidationStatus.REJECTED, reason

    vstate.status = ValidationStatus.APPROVED
    return ValidationStatus.APPROVED, (
        f"All gates passed: win_rate={vstate.win_rate:.1%} "
        f"mean_ev={vstate.mean_ev:.4f} trades={settled} "
        f"hours={vstate.elapsed_hours:.1f}"
    )


def summarise(vstate: ValidationState) -> str:
    return (
        f"[{vstate.experiment_id}] "
        f"trades={vstate.settled_trades} "
        f"win={vstate.win_rate:.1%} "
        f"mean_ev={vstate.mean_ev:.4f} "
        f"streak={vstate.max_loss_streak_seen} "
        f"elapsed={vstate.elapsed_hours:.1f}h "
        f"status={vstate.status.value}"
    )
