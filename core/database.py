"""
Database — SQLite persistence layer
=====================================
Single source of truth for all historical data:
  - trades    : every opened/closed position with full PnL
  - ticks     : sampled BTC + Polymarket prices for backtesting
  - decisions : every agent action and its outcome
  - performance: rolling performance snapshots
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

logger = logging.getLogger(__name__)

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(_BASE, "trading_bot.db")

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS trades (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp        TEXT    NOT NULL,
    market_id        TEXT,
    condition_id     TEXT,
    token_side       TEXT,           -- UP / DOWN
    token_id         TEXT,
    entry_price      REAL,           -- price paid per share (0-1)
    entry_size_usdc  REAL,           -- USDC spent
    size_shares      REAL,           -- shares bought
    exit_price       REAL,           -- price received per share at close
    exit_type        TEXT,           -- EXPIRY / TAKE_PROFIT / STOP_LOSS / DRY_RUN
    pnl_usd          REAL,           -- signed P&L in USDC
    pnl_pct          REAL,           -- signed P&L as % of capital risked
    win              INTEGER,        -- 1 = win, 0 = loss
    strategy_version INTEGER DEFAULT 1,
    dry_run          INTEGER DEFAULT 1  -- 1 = paper, 0 = live
);

CREATE TABLE IF NOT EXISTS ticks (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp  TEXT    NOT NULL,
    market_id  TEXT,
    btc_price  REAL,
    up_ask     REAL,
    down_ask   REAL,
    up_bid     REAL,
    down_bid   REAL
);

CREATE TABLE IF NOT EXISTS decisions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp     TEXT    NOT NULL,
    agent         TEXT    NOT NULL,   -- POSITION_MANAGER / EVALUATOR / RESEARCHER / DEVELOPER
    decision_type TEXT    NOT NULL,   -- SNIPE / EARLY_EXIT / HOLD / REJECT / PROMOTE / etc.
    details       TEXT,               -- JSON blob
    outcome       TEXT                -- filled in later
);

CREATE TABLE IF NOT EXISTS performance (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp        TEXT    NOT NULL,
    total_trades     INTEGER,
    win_rate         REAL,
    total_pnl_usd    REAL,
    avg_pnl_pct      REAL,
    max_drawdown     REAL,
    strategy_version INTEGER
);

CREATE INDEX IF NOT EXISTS idx_trades_ts     ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_ticks_ts      ON ticks(timestamp);
CREATE INDEX IF NOT EXISTS idx_decisions_ts  ON decisions(timestamp);
"""


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def init_db(path: str = DB_PATH) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(_SCHEMA)
    logger.info("Database initialised at %s", path)


@contextmanager
def get_conn(path: str = DB_PATH) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Trade logging
# ---------------------------------------------------------------------------

def log_trade(
    market_id: str,
    condition_id: str,
    token_side: str,
    token_id: str,
    entry_price: float,
    entry_size_usdc: float,
    size_shares: float,
    exit_price: float,
    exit_type: str,           # EXPIRY / TAKE_PROFIT / STOP_LOSS / DRY_RUN
    pnl_usd: float,
    pnl_pct: float,
    win: bool,
    strategy_version: int = 1,
    dry_run: bool = True,
    path: str = DB_PATH,
) -> int:
    ts = datetime.now(tz=timezone.utc).isoformat()
    with get_conn(path) as conn:
        cur = conn.execute(
            """INSERT INTO trades
               (timestamp,market_id,condition_id,token_side,token_id,
                entry_price,entry_size_usdc,size_shares,exit_price,exit_type,
                pnl_usd,pnl_pct,win,strategy_version,dry_run)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (ts, market_id, condition_id, token_side, token_id,
             entry_price, entry_size_usdc, size_shares, exit_price, exit_type,
             pnl_usd, pnl_pct, int(win), strategy_version, int(dry_run)),
        )
        return cur.lastrowid or 0


# ---------------------------------------------------------------------------
# Tick logging (sampled, not every message)
# ---------------------------------------------------------------------------

def log_tick(
    market_id: str,
    btc_price: float,
    up_ask: float,
    down_ask: float,
    up_bid: float,
    down_bid: float,
    path: str = DB_PATH,
) -> None:
    ts = datetime.now(tz=timezone.utc).isoformat()
    with get_conn(path) as conn:
        conn.execute(
            """INSERT INTO ticks (timestamp,market_id,btc_price,up_ask,down_ask,up_bid,down_bid)
               VALUES (?,?,?,?,?,?,?)""",
            (ts, market_id, btc_price, up_ask, down_ask, up_bid, down_bid),
        )


# ---------------------------------------------------------------------------
# Decision logging
# ---------------------------------------------------------------------------

def log_decision(
    agent: str,
    decision_type: str,
    details: dict,
    path: str = DB_PATH,
) -> int:
    ts = datetime.now(tz=timezone.utc).isoformat()
    with get_conn(path) as conn:
        cur = conn.execute(
            "INSERT INTO decisions (timestamp,agent,decision_type,details) VALUES (?,?,?,?)",
            (ts, agent, decision_type, json.dumps(details)),
        )
        return cur.lastrowid or 0


def update_decision_outcome(decision_id: int, outcome: str, path: str = DB_PATH) -> None:
    with get_conn(path) as conn:
        conn.execute(
            "UPDATE decisions SET outcome=? WHERE id=?", (outcome, decision_id)
        )


# ---------------------------------------------------------------------------
# Performance snapshots
# ---------------------------------------------------------------------------

def log_performance(
    total_trades: int,
    win_rate: float,
    total_pnl_usd: float,
    avg_pnl_pct: float,
    max_drawdown: float,
    strategy_version: int = 1,
    path: str = DB_PATH,
) -> None:
    ts = datetime.now(tz=timezone.utc).isoformat()
    with get_conn(path) as conn:
        conn.execute(
            """INSERT INTO performance
               (timestamp,total_trades,win_rate,total_pnl_usd,avg_pnl_pct,max_drawdown,strategy_version)
               VALUES (?,?,?,?,?,?,?)""",
            (ts, total_trades, win_rate, total_pnl_usd, avg_pnl_pct, max_drawdown, strategy_version),
        )


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def fetch_recent_trades(n: int = 50, dry_run: bool | None = None, path: str = DB_PATH) -> list[dict]:
    with get_conn(path) as conn:
        if dry_run is None:
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (n,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM trades WHERE dry_run=? ORDER BY id DESC LIMIT ?",
                (int(dry_run), n),
            ).fetchall()
    return [dict(r) for r in rows]


def fetch_resolved_trades_with_context(path: str = DB_PATH) -> list[dict]:
    """
    Returns every closed trade enriched with z_score and p_true from the
    matching SNIPE decision.  This is the Reviewer's primary data source —
    outcomes here are always resolved (written at position close time).

    Joins strategy:
      - Load all trades (float P&L, win flag)
      - Load all SNIPE decisions that have an outcome set (1-to-1 with trades)
      - Match on market_id in Python to avoid SQLite json_extract version issues
    """
    with get_conn(path) as conn:
        trade_rows = conn.execute(
            "SELECT * FROM trades ORDER BY id ASC"
        ).fetchall()

        dec_rows = conn.execute(
            """SELECT id, timestamp, details, outcome
               FROM decisions
               WHERE agent = 'POSITION_MANAGER'
                 AND decision_type = 'SNIPE'
                 AND outcome IS NOT NULL
                 AND outcome != ''
               ORDER BY id ASC"""
        ).fetchall()

    trades    = [dict(r) for r in trade_rows]
    decisions = [dict(r) for r in dec_rows]

    # Build market_id → decision context (last decision wins per market_id)
    ctx_by_market: dict[str, dict] = {}
    for dec in decisions:
        try:
            details: dict = json.loads(dec.get("details") or "{}")
        except (json.JSONDecodeError, TypeError):
            details = {}
        mid = str(details.get("market_id", ""))
        if mid:
            ctx_by_market[mid] = {
                "p_true":  float(details.get("p_true",  0.5)),
                "z_score": float(details.get("z_score", 0.0)),
            }

    # Enrich each trade row
    enriched: list[dict] = []
    for t in trades:
        mid = str(t.get("market_id", ""))
        ctx = ctx_by_market.get(mid, {"p_true": 0.5, "z_score": 0.0})
        enriched.append({
            **t,
            "p_true":  ctx["p_true"],
            "z_score": ctx["z_score"],
            # Normalise field names so Reviewer.Trade constructor is unambiguous
            "ask_price":   float(t.get("entry_price") or 0.0),
            "actual_pnl":  float(t.get("pnl_usd")     or 0.0),
            "outcome":     "WIN" if t.get("win") else "LOSS",
        })

    return enriched


def fetch_recent_ticks(market_id: str, n: int = 360, path: str = DB_PATH) -> list[dict]:
    with get_conn(path) as conn:
        rows = conn.execute(
            "SELECT * FROM ticks WHERE market_id=? ORDER BY id DESC LIMIT ?",
            (market_id, n),
        ).fetchall()
    return [dict(r) for r in reversed(rows)]
