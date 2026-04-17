"""
Trade Tracker — Permanent CSV trade history
============================================
Appends every closed trade to trades_history.csv at the project root.
All fields needed by the dashboard are written at close time so the
dashboard has zero dependency on trading_bot.db.
"""
from __future__ import annotations

import csv
import os
import threading
from dataclasses import dataclass

_BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(_BASE, "trades_history.csv")
_LOCK    = threading.Lock()

FIELDS = [
    "trade_id",
    "entry_timestamp",
    "exit_timestamp",
    "token_side",
    "entry_price",
    "exit_price",
    "size_usdc",
    "pnl_usd",
    "pnl_pct",
    "outcome",
    "strategy_name",
    "exit_reason",
    "market_id",
    "dry_run",
]


@dataclass
class TradeEntry:
    trade_id:        int
    entry_timestamp: str   # ISO-8601 wall-clock at entry
    exit_timestamp:  str   # ISO-8601 wall-clock at close
    token_side:      str   # UP / DOWN
    entry_price:     float # share price paid
    exit_price:      float # share price received
    size_usdc:       float # USDC risked
    pnl_usd:         float # signed dollar P&L
    pnl_pct:         float # signed % P&L on share price
    outcome:         str   # WIN / LOSS
    strategy_name:   str   # e.g. "v2-baseline" or "v3-a1b2c3d4"
    exit_reason:     str   # EXPIRY / TAKE_PROFIT / STOP_LOSS
    market_id:       str
    dry_run:         bool


def record_trade(entry: TradeEntry) -> None:
    """Append one trade row to trades_history.csv (thread-safe)."""
    with _LOCK:
        write_header = not os.path.exists(CSV_PATH)
        with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            if write_header:
                writer.writeheader()
            writer.writerow({
                "trade_id":        entry.trade_id,
                "entry_timestamp": entry.entry_timestamp,
                "exit_timestamp":  entry.exit_timestamp,
                "token_side":      entry.token_side,
                "entry_price":     round(entry.entry_price, 6),
                "exit_price":      round(entry.exit_price, 6),
                "size_usdc":       round(entry.size_usdc, 4),
                "pnl_usd":         round(entry.pnl_usd, 6),
                "pnl_pct":         round(entry.pnl_pct, 4),
                "outcome":         entry.outcome,
                "strategy_name":   entry.strategy_name,
                "exit_reason":     entry.exit_reason,
                "market_id":       entry.market_id,
                "dry_run":         int(entry.dry_run),
            })
