"""
Developer — Safe Parameter Updater
====================================
Writes validated strategy configurations to disk atomically.
Never touches .py source files — all tuneable behaviour lives in
strategy_config.json which the engine reads at runtime.

Promotion pipeline:
  candidate_config.json  →  (validator approves)  →  strategy_config.json

All historical experiments are appended to learning_history.json for audit.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LIVE_CONFIG_PATH      = os.path.join(_BASE, "strategy_config.json")
CANDIDATE_CONFIG_PATH = os.path.join(_BASE, "candidate_config.json")
HISTORY_PATH          = os.path.join(_BASE, "learning_history.json")


# ---------------------------------------------------------------------------
# Atomic file I/O helpers
# ---------------------------------------------------------------------------

def _atomic_write(path: str, data: dict) -> None:
    """Write JSON atomically: write to .tmp then rename."""
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    shutil.move(tmp, path)


def load_live_config() -> dict:
    with open(LIVE_CONFIG_PATH) as f:
        return json.load(f)


def load_candidate_config() -> dict | None:
    if not os.path.exists(CANDIDATE_CONFIG_PATH):
        return None
    with open(CANDIDATE_CONFIG_PATH) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# History log
# ---------------------------------------------------------------------------

def _append_history(entry: dict) -> None:
    history: list[dict] = []
    if os.path.exists(HISTORY_PATH):
        try:
            with open(HISTORY_PATH) as f:
                history = json.load(f)
        except (json.JSONDecodeError, IOError):
            history = []
    history.append(entry)
    _atomic_write(HISTORY_PATH, history)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def stage_candidate(
    params: dict,
    backtest_score: float,
    backtest_win_rate: float,
    backtest_trades: int,
    experiment_id: str,
) -> None:
    """
    Write a validated-by-backtest candidate to candidate_config.json.
    Does NOT modify the live config.
    """
    candidate = dict(params)
    candidate["_meta"] = {
        "version": _next_version(),
        "experiment_id": experiment_id,
        "staged_at": datetime.now(tz=timezone.utc).isoformat(),
        "backtest_score": round(backtest_score, 6),
        "backtest_win_rate": round(backtest_win_rate, 4),
        "backtest_trades": backtest_trades,
        "status": "staged",
        "promoted_at": None,
    }
    _atomic_write(CANDIDATE_CONFIG_PATH, candidate)
    logger.info(
        "Developer: staged candidate %s (backtest win_rate=%.1f%%, score=%.4f)",
        experiment_id, backtest_win_rate, backtest_score,
    )
    _append_history({
        "event": "staged",
        "experiment_id": experiment_id,
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "backtest_win_rate": backtest_win_rate,
        "backtest_score": backtest_score,
        "params": params,
    })


def _auto_git_commit(
    candidate: dict,
    experiment_id: str,
    paper_win_rate: float,
    paper_trades: int,
) -> None:
    """Commit the new strategy_config.json to git and push."""
    old_cfg = {}
    try:
        history = json.load(open(HISTORY_PATH)) if os.path.exists(HISTORY_PATH) else []
        archived = [e for e in history if e.get("event") == "archived_live"]
        if archived:
            old_cfg = archived[-1].get("config", {})
    except Exception:
        pass

    changes = diff_configs(old_cfg, candidate) or ["  (initial promotion)"]
    changes_text = "\n".join(changes)

    version = candidate.get("_meta", {}).get("version", "?")
    bt_wr   = candidate.get("_meta", {}).get("backtest_win_rate", 0)
    bt_n    = candidate.get("_meta", {}).get("backtest_trades", 0)

    msg = (
        f"Auto-Optimization: Promote strategy v{version} [{experiment_id}]\n\n"
        f"Paper trading: {paper_win_rate:.1%} win rate over {paper_trades} trades\n"
        f"Backtest:      {bt_wr:.1%} win rate over {bt_n} trades\n\n"
        f"Parameter changes:\n{changes_text}"
    )

    try:
        repo_root = _BASE
        subprocess.run(
            ["git", "add", "strategy_config.json", "learning_history.json"],
            cwd=repo_root, check=True, capture_output=True,
        )
        subprocess.run(
            ["git", "commit", "-m", msg],
            cwd=repo_root, check=True, capture_output=True,
        )
        result = subprocess.run(
            ["git", "push"],
            cwd=repo_root, capture_output=True, text=True,
        )
        if result.returncode == 0:
            logger.info("Auto-Git: committed and pushed strategy v%s", version)
        else:
            logger.warning("Auto-Git: commit succeeded but push failed: %s", result.stderr.strip())
    except subprocess.CalledProcessError as exc:
        logger.warning("Auto-Git: git operation failed: %s", exc.stderr.decode(errors="replace").strip() if exc.stderr else str(exc))
    except FileNotFoundError:
        logger.warning("Auto-Git: git not found — skipping version control")


def promote_candidate(
    paper_win_rate: float,
    paper_trades: int,
    experiment_id: str,
) -> bool:
    """
    Replace live strategy_config.json with the staged candidate.
    Called only after the Validator approves paper trading results.
    Returns True on success.
    """
    candidate = load_candidate_config()
    if candidate is None:
        logger.error("Developer: no candidate config found — cannot promote")
        return False

    # Archive the current live config first
    live = load_live_config()
    _append_history({
        "event": "archived_live",
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "version": live.get("_meta", {}).get("version"),
        "config": live,
    })

    # Update candidate metadata and write as new live config
    candidate["_meta"]["promoted_at"] = datetime.now(tz=timezone.utc).isoformat()
    candidate["_meta"]["paper_win_rate"] = round(paper_win_rate, 4)
    candidate["_meta"]["paper_trades"] = paper_trades
    candidate["_meta"]["status"] = "live"
    candidate["_meta"]["baseline"] = False

    _atomic_write(LIVE_CONFIG_PATH, candidate)

    # Clean up candidate file
    if os.path.exists(CANDIDATE_CONFIG_PATH):
        os.remove(CANDIDATE_CONFIG_PATH)

    _append_history({
        "event": "promoted",
        "experiment_id": experiment_id,
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "paper_win_rate": paper_win_rate,
        "paper_trades": paper_trades,
        "new_config": candidate,
    })

    logger.info(
        "Developer: PROMOTED candidate %s → live (paper win_rate=%.1f%%, %d trades)",
        experiment_id, paper_win_rate, paper_trades,
    )

    _auto_git_commit(candidate, experiment_id, paper_win_rate, paper_trades)
    return True


def reject_candidate(reason: str, experiment_id: str) -> None:
    """Discard a candidate that failed paper trading validation."""
    _append_history({
        "event": "rejected",
        "experiment_id": experiment_id,
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "reason": reason,
    })
    if os.path.exists(CANDIDATE_CONFIG_PATH):
        os.remove(CANDIDATE_CONFIG_PATH)
    logger.info("Developer: rejected candidate %s — %s", experiment_id, reason)


def diff_configs(old: dict, new: dict, prefix: str = "") -> list[str]:
    """Return human-readable list of changed parameters."""
    changes: list[str] = []
    for k, v in new.items():
        if k == "_meta":
            continue
        full_key = f"{prefix}{k}" if prefix else k
        if isinstance(v, dict):
            changes.extend(diff_configs(old.get(k, {}), v, prefix=full_key + "."))
        else:
            old_v = old.get(k)
            if old_v != v:
                changes.append(f"  {full_key}: {old_v!r} → {v!r}")
    return changes


def _next_version() -> int:
    try:
        live = load_live_config()
        return int(live.get("_meta", {}).get("version", 0)) + 1
    except Exception:
        return 1
