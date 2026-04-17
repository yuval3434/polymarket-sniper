"""
Reviewer — Data Ingestion & Error Analysis
==========================================
Parses closed trade history, classifies loss patterns, and returns a
structured AnalysisReport that the Researcher uses to form hypotheses.

Primary data source : trading_bot.db  (outcomes always resolved at close time)
Fallback            : paper_trades.csv (only has resolved outcomes after --resolve)
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
    timestamp:           str
    market_id:           str
    btc_price:           float
    opening_price:       float
    token_side:          str    # "UP" or "DOWN"
    ask_price:           float
    p_true:              float
    z_score:             float
    expected_value:      float
    would_execute:       bool
    fee_adjusted_profit: float
    actual_outcome:      str    # "WIN", "LOSS", "PENDING"
    actual_pnl:          float


@dataclass
class LossPattern:
    name:          str
    count:         int
    description:   str
    suggested_fix: dict


@dataclass
class AnalysisReport:
    total_trades:        int
    executed_trades:     int
    wins:                int
    losses:              int
    pending:             int
    win_rate:            float
    avg_pnl:             float
    avg_z_score_wins:    float
    avg_z_score_losses:  float
    avg_ask_wins:        float
    avg_ask_losses:      float
    patterns:            list[LossPattern] = field(default_factory=list)
    raw_trades:          list[Trade]       = field(default_factory=list)

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
# Primary loader — trading_bot.db (outcomes always resolved)
# ---------------------------------------------------------------------------

def _load_trades_from_db() -> list[Trade]:
    """
    Reads from trading_bot.db via fetch_resolved_trades_with_context().
    Each row already has p_true, z_score (joined from decisions table)
    and actual P&L from the trades table.
    """
    try:
        from core.database import fetch_resolved_trades_with_context
        rows = fetch_resolved_trades_with_context()
    except Exception as exc:
        logger.warning("Reviewer: DB query failed (%s) — will try CSV fallback", exc)
        return []

    logger.debug("Reviewer [DB]: fetched %d raw rows from trading_bot.db", len(rows))

    trades: list[Trade] = []
    for i, row in enumerate(rows):
        logger.debug(
            "Reviewer [DB] row %d: market=%s side=%s ask=%.4f p_true=%.4f "
            "z=%.4f pnl_usd=%.4f win=%s exit=%s",
            i,
            row.get("market_id", "?"),
            row.get("token_side", "?"),
            float(row.get("ask_price") or 0),
            float(row.get("p_true") or 0.5),
            float(row.get("z_score") or 0),
            float(row.get("actual_pnl") or 0),
            row.get("outcome", "?"),
            row.get("exit_type", "?"),
        )
        try:
            trades.append(Trade(
                timestamp=str(row.get("timestamp", "")),
                market_id=str(row.get("market_id", "")),
                btc_price=0.0,       # not stored in trades table
                opening_price=0.0,   # not stored in trades table
                token_side=str(row.get("token_side", "")),
                ask_price=float(row.get("ask_price") or 0.0),
                p_true=float(row.get("p_true") or 0.5),
                z_score=float(row.get("z_score") or 0.0),
                expected_value=0.0,  # not stored in trades table
                would_execute=True,  # all DB rows are executed trades
                fee_adjusted_profit=float(row.get("actual_pnl") or 0.0),
                actual_outcome=str(row.get("outcome", "PENDING")),
                actual_pnl=float(row.get("actual_pnl") or 0.0),
            ))
        except (TypeError, ValueError) as exc:
            logger.warning("Reviewer [DB]: skipped row %d due to parse error: %s | row=%s", i, exc, row)

    logger.info("Reviewer [DB]: loaded %d trades (source: trading_bot.db)", len(trades))
    return trades


# ---------------------------------------------------------------------------
# Fallback loader — paper_trades.csv
# (only useful after `py main.py --resolve` has been run)
# ---------------------------------------------------------------------------

def _load_trades_from_csv(csv_path: str) -> list[Trade]:
    trades: list[Trade] = []
    if not os.path.exists(csv_path):
        logger.debug("Reviewer [CSV]: file not found at %s", csv_path)
        return trades

    with open(csv_path, newline="", encoding="utf-8") as f:
        raw_rows = list(csv.DictReader(f))

    logger.debug("Reviewer [CSV]: read %d raw rows from %s", len(raw_rows), csv_path)

    for i, row in enumerate(raw_rows):
        logger.debug(
            "Reviewer [CSV] row %d: side=%s ask=%s P_true=%s Z_score=%s outcome=%s pnl=%s",
            i,
            row.get("token_side", "?"),
            row.get("ask_price", "?"),
            row.get("P_true", "?"),
            row.get("Z_score", "?"),
            row.get("actual_outcome", "?"),
            row.get("actual_pnl", "?"),
        )
        try:
            trades.append(Trade(
                timestamp=row.get("timestamp", ""),
                market_id=row.get("market_id", ""),
                btc_price=float(row.get("btc_price") or 0),
                opening_price=float(row.get("opening_price") or 0),
                token_side=row.get("token_side", ""),
                ask_price=float(row.get("ask_price") or 0),
                p_true=float(row.get("P_true") or 0.5),
                z_score=float(row.get("Z_score") or 0),
                expected_value=float(row.get("expected_value") or 0),
                would_execute=row.get("would_execute", "False").strip().lower() == "true",
                fee_adjusted_profit=float(row.get("fee_adjusted_profit_if_right") or 0),
                actual_outcome=row.get("actual_outcome", "PENDING"),
                actual_pnl=float(row.get("actual_pnl") or 0),
            ))
        except (TypeError, ValueError) as exc:
            logger.warning("Reviewer [CSV]: skipped row %d due to parse error: %s | row=%s", i, exc, row)

    resolved = sum(1 for t in trades if t.actual_outcome in ("WIN", "LOSS"))
    logger.info(
        "Reviewer [CSV]: loaded %d rows (%d resolved, %d pending) from %s",
        len(trades), resolved, len(trades) - resolved, csv_path,
    )
    return trades


# ---------------------------------------------------------------------------
# Pattern detectors
# ---------------------------------------------------------------------------

def _detect_low_z_losses(losses: list[Trade]) -> Optional[LossPattern]:
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
    loss_up   = sum(1 for t in losses if t.token_side == "UP")
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
    """
    Load trades and return a full AnalysisReport.

    Tries trading_bot.db first (always has resolved outcomes).
    Falls back to paper_trades.csv if the DB is empty.
    """
    # ── Primary: DB ──────────────────────────────────────────────────────────
    all_trades = _load_trades_from_db()

    # ── Fallback: CSV ────────────────────────────────────────────────────────
    if not all_trades:
        logger.info("Reviewer: DB returned 0 trades, falling back to CSV at %s", csv_path)
        all_trades = _load_trades_from_csv(csv_path)

    # ── Filter ───────────────────────────────────────────────────────────────
    # DB trades: would_execute=True always; CSV trades: honour the flag
    executed = [t for t in all_trades if t.would_execute]
    wins     = [t for t in executed if t.actual_outcome == "WIN"]
    losses   = [t for t in executed if t.actual_outcome == "LOSS"]
    pending  = [t for t in executed if t.actual_outcome == "PENDING"]

    logger.debug(
        "Reviewer filter: total=%d executed=%d wins=%d losses=%d pending=%d",
        len(all_trades), len(executed), len(wins), len(losses), len(pending),
    )

    def safe_mean(vals: list[float]) -> float:
        return sum(vals) / len(vals) if vals else 0.0

    win_rate = len(wins) / len(executed) if executed else 0.0

    patterns: list[LossPattern] = []
    for detector in (_detect_low_z_losses, _detect_high_ask_losses, _detect_late_entry):
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
