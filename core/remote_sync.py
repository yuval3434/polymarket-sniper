"""
Remote trade sync — pulls trades from peer bot instances and caches them locally.
Called synchronously from the reviewer (which already runs in a thread executor).

Each peer's trades are stored in a local `remote_trades` table (separate from the
main `trades` table) and de-duplicated by (source_url, timestamp, market_id, token_side).
The reviewer includes these rows so the learning pipeline learns from every machine.
"""
from __future__ import annotations

import json
import logging
import time
from urllib.error import URLError
from urllib.request import urlopen

from config import REMOTE_BOT_PEERS
from core.database import DB_PATH, get_conn

logger = logging.getLogger(__name__)

_SYNC_TIMEOUT_S = 3

_SCHEMA = """
CREATE TABLE IF NOT EXISTS remote_trades (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url       TEXT    NOT NULL,
    timestamp        TEXT    NOT NULL,
    market_id        TEXT,
    token_side       TEXT,
    entry_price      REAL,
    entry_size_usdc  REAL,
    size_shares      REAL,
    exit_price       REAL,
    exit_type        TEXT,
    pnl_usd          REAL,
    pnl_pct          REAL,
    win              INTEGER,
    p_true           REAL,
    z_score          REAL,
    dry_run          INTEGER DEFAULT 0,
    synced_at        TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_remote_dedup
    ON remote_trades(source_url, timestamp, market_id, token_side);
"""


def _ensure_table() -> None:
    with get_conn() as conn:
        conn.executescript(_SCHEMA)


def sync_remote_trades() -> int:
    """
    Fetch trades from all REMOTE_BOT_PEERS and store new ones locally.
    Returns total new trades imported across all peers.
    Safe to call from a thread (uses stdlib urllib, no asyncio).
    """
    if not REMOTE_BOT_PEERS:
        return 0

    _ensure_table()

    total_new = 0
    for peer_url in REMOTE_BOT_PEERS:
        try:
            url = peer_url.rstrip("/") + "/trades"
            with urlopen(url, timeout=_SYNC_TIMEOUT_S) as resp:
                trades = json.loads(resp.read())
            new = _merge(peer_url, trades)
            total_new += new
            if new:
                logger.info("Remote sync: +%d new trades from %s", new, peer_url)
            else:
                logger.debug("Remote sync: no new trades from %s", peer_url)
        except (URLError, OSError, json.JSONDecodeError, Exception) as exc:
            logger.warning("Remote sync: could not reach %s — %s", peer_url, exc)

    return total_new


def _merge(source_url: str, trades: list[dict]) -> int:
    """Insert trades not already cached. Returns count of newly inserted rows."""
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    new_count = 0
    with get_conn() as conn:
        for t in trades:
            try:
                cur = conn.execute(
                    """INSERT OR IGNORE INTO remote_trades
                       (source_url,timestamp,market_id,token_side,
                        entry_price,entry_size_usdc,size_shares,exit_price,exit_type,
                        pnl_usd,pnl_pct,win,p_true,z_score,dry_run,synced_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        source_url,
                        t.get("timestamp", ""),
                        t.get("market_id", ""),
                        t.get("token_side", ""),
                        float(t.get("entry_price") or 0),
                        float(t.get("entry_size_usdc") or 0),
                        float(t.get("size_shares") or 0),
                        float(t.get("exit_price") or 0),
                        t.get("exit_type", "REMOTE"),
                        float(t.get("pnl_usd") or 0),
                        float(t.get("pnl_pct") or 0),
                        int(t.get("win") or 0),
                        float(t.get("p_true") or 0.5),
                        float(t.get("z_score") or 0),
                        int(t.get("dry_run") or 0),
                        now,
                    ),
                )
                if cur.rowcount > 0:
                    new_count += 1
            except Exception as exc:
                logger.debug("Remote sync: skipped one trade from %s: %s", source_url, exc)
    return new_count


def fetch_remote_trades_cached() -> list[dict]:
    """Return all previously synced remote trades from the local cache table."""
    try:
        _ensure_table()
        with get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM remote_trades ORDER BY timestamp ASC"
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.warning("Could not read remote_trades table: %s", exc)
        return []
