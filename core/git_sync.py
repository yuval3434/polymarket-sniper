"""
Git Sync — shared learning data across all machines.

Every GIT_SYNC_INTERVAL seconds the bot:
  1. Exports trading_bot.db trades → trades_history.csv
  2. git pull (merge remote changes)
  3. Imports any new rows from trades_history.csv → local DB (dedup)
  4. git add + commit + push the updated data files

Files synced via git (all human-readable, merge-friendly):
  trades_history.csv         — full trade export (deduped)
  paper_trades.csv           — paper trade log
  candidate_paper_trades.csv — shadow trades for candidate validation
  candidate_config.json      — current candidate strategy
  learning_history.json      — experiment audit log
  strategy_config.json       — live strategy (already tracked)
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import subprocess
import time

logger = logging.getLogger(__name__)

_BASE              = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TRADES_CSV        = os.path.join(_BASE, "trades_history.csv")
GIT_SYNC_INTERVAL  = 600   # seconds between sync cycles (10 min)

_DATA_FILES = [
    "trades_history.csv",
    "paper_trades.csv",
    "candidate_paper_trades.csv",
    "candidate_config.json",
    "learning_history.json",
    "strategy_config.json",
]

_TRADE_FIELDNAMES = [
    "timestamp", "market_id", "condition_id", "token_side", "token_id",
    "entry_price", "entry_size_usdc", "size_shares", "exit_price", "exit_type",
    "pnl_usd", "pnl_pct", "win", "strategy_version", "dry_run",
]


# ---------------------------------------------------------------------------
# Export DB → CSV
# ---------------------------------------------------------------------------

def export_trades_to_csv() -> int:
    """Write all trades from trading_bot.db into trades_history.csv. Returns row count."""
    try:
        from core.database import get_conn
        with get_conn() as conn:
            rows = conn.execute("SELECT * FROM trades ORDER BY id ASC").fetchall()
        trades = [dict(r) for r in rows]
    except Exception as exc:
        logger.warning("git_sync: DB export failed — %s", exc)
        return 0

    if not trades:
        return 0

    with open(_TRADES_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_TRADE_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(trades)

    logger.debug("git_sync: exported %d trades to trades_history.csv", len(trades))
    return len(trades)


# ---------------------------------------------------------------------------
# Import CSV → remote_trades (INSERT OR IGNORE = guaranteed dedup)
# ---------------------------------------------------------------------------

def import_trades_from_csv() -> int:
    """
    Import rows from trades_history.csv into the remote_trades table.
    Uses INSERT OR IGNORE against the UNIQUE index — no Python-side dedup needed.
    Does NOT touch the main trades table (local executions only).
    """
    if not os.path.exists(_TRADES_CSV):
        return 0

    try:
        with open(_TRADES_CSV, newline="") as f:
            rows = list(csv.DictReader(f))
    except Exception as exc:
        logger.warning("git_sync: CSV read failed — %s", exc)
        return 0

    if not rows:
        return 0

    try:
        from core.remote_sync import _ensure_table, _SCHEMA
        from core.database import get_conn
        _ensure_table()

        synced_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        new_count = 0
        source_url = "git"

        with get_conn() as conn:
            for r in rows:
                try:
                    cur = conn.execute(
                        """INSERT OR IGNORE INTO remote_trades
                           (source_url,timestamp,market_id,token_side,
                            entry_price,entry_size_usdc,size_shares,exit_price,exit_type,
                            pnl_usd,pnl_pct,win,p_true,z_score,dry_run,synced_at)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            source_url,
                            r.get("timestamp", ""),
                            r.get("market_id", ""),
                            r.get("token_side", ""),
                            float(r.get("entry_price") or 0),
                            float(r.get("entry_size_usdc") or 0),
                            float(r.get("size_shares") or 0),
                            float(r.get("exit_price") or 0),
                            r.get("exit_type", ""),
                            float(r.get("pnl_usd") or 0),
                            float(r.get("pnl_pct") or 0),
                            int(r.get("win") or 0),
                            0.5,   # p_true not stored in trades table
                            0.0,   # z_score not stored in trades table
                            int(r.get("dry_run") or 0),
                            synced_at,
                        ),
                    )
                    if cur.rowcount > 0:
                        new_count += 1
                except Exception:
                    pass

        if new_count:
            logger.info("git_sync: imported %d new trades from git", new_count)
        return new_count

    except Exception as exc:
        logger.warning("git_sync: import failed — %s", exc)
        return 0


# ---------------------------------------------------------------------------
# Git operations
# ---------------------------------------------------------------------------

def _run_git(args: list[str], timeout: int = 10) -> tuple[bool, str]:
    """Run a git command in the repo root. Returns (success, output)."""
    try:
        result = subprocess.run(
            ["git"] + args,
            cwd=_BASE,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.returncode == 0, (result.stdout + result.stderr).strip()
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except Exception as exc:
        return False, str(exc)


def git_pull() -> bool:
    # Commit any local data changes first so rebase has a clean working tree
    files_to_add = [f for f in _DATA_FILES if os.path.exists(os.path.join(_BASE, f))]
    if files_to_add:
        _run_git(["add"] + files_to_add)
        ok, _ = _run_git(["diff", "--cached", "--quiet"])
        if not ok:
            hostname = os.environ.get("HOSTNAME", "bot")
            _run_git(["commit", "-m", f"[auto-sync] pre-pull commit from {hostname}"])

    ok, out = _run_git(["pull", "--rebase", "origin", "HEAD"])
    if ok:
        logger.info("git_sync: pull OK — %s", out.splitlines()[0] if out else "up to date")
    else:
        logger.warning("git_sync: pull failed — %s", out)
    return ok


def git_push() -> bool:
    """Stage data files, commit if anything changed, then push."""
    files_to_add = [f for f in _DATA_FILES if os.path.exists(os.path.join(_BASE, f))]
    if not files_to_add:
        return True

    _run_git(["add"] + files_to_add)

    ok, _ = _run_git(["diff", "--cached", "--quiet"])
    if ok:
        logger.debug("git_sync: nothing changed — skip commit")
        return True

    hostname = os.environ.get("HOSTNAME", "bot")
    msg = f"[auto-sync] learning data from {hostname} — {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}"
    ok, out = _run_git(["commit", "-m", msg])
    if not ok:
        logger.warning("git_sync: commit failed — %s", out)
        return False

    ok, out = _run_git(["push", "origin", "HEAD"], timeout=15)
    if ok:
        logger.info("git_sync: pushed learning data to git")
    else:
        logger.warning("git_sync: push failed (will retry next cycle) — %s", out)
    return ok


# ---------------------------------------------------------------------------
# Main async loop
# ---------------------------------------------------------------------------

async def push_trade_now() -> None:
    """
    Immediately export trades to CSV and push to git.
    Called after every closed trade so data is never more than one trade behind.
    Skips pull to avoid blocking the event loop — the periodic git_sync_loop handles merges.
    """
    try:
        await asyncio.to_thread(export_trades_to_csv)
        await asyncio.to_thread(git_push)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning("git_sync: immediate push failed — %s", exc)


async def git_sync_loop() -> None:
    """Runs every GIT_SYNC_INTERVAL seconds. Exports, pulls, imports, pushes."""
    logger.info("git_sync: loop started (interval=%ds) — first sync in 30s", GIT_SYNC_INTERVAL)

    # Delay first sync so the event loop is fully running before we hit the network
    await asyncio.sleep(30)

    while True:
        try:
            await asyncio.to_thread(export_trades_to_csv)
            await asyncio.to_thread(git_pull)
            await asyncio.to_thread(import_trades_from_csv)
            await asyncio.to_thread(git_push)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("git_sync: cycle error — %s", exc)
        await asyncio.sleep(GIT_SYNC_INTERVAL)
