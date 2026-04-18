import asyncio
import csv
import logging
import os
import time
from datetime import datetime, timezone

import aiohttp

from config import GAMMA_API_BASE, PAPER_TRADE_CSV
from market.state import MarketState

logger = logging.getLogger(__name__)

_FIELDNAMES = [
    "timestamp",
    "market_id",
    "condition_id",
    "btc_price",
    "opening_price",
    "token_side",
    "token_id",
    "ask_price",
    "P_true",
    "Z_score",
    "expected_value",
    "would_execute",
    "fee_adjusted_profit_if_right",
    "actual_outcome",   # WIN / LOSS / PENDING — filled in post-resolve
    "actual_pnl",       # filled in post-resolve
]


def _csv_path() -> str:
    return PAPER_TRADE_CSV


def _ensure_csv_header() -> None:
    path = _csv_path()
    if not os.path.exists(path):
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=_FIELDNAMES)
            writer.writeheader()


def _write_decision_row(row: dict) -> None:
    """Synchronous CSV append — runs in a thread executor to avoid blocking the event loop."""
    _ensure_csv_header()
    with open(_csv_path(), "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDNAMES)
        writer.writerow(row)


def log_decision(
    state: MarketState,
    token_side: str,         # "UP" or "DOWN"
    token_id: str,
    ask_price: float,
    p_true: float,
    z_score: float,
    ev: float,
    would_execute: bool,
) -> None:
    """Append a single snipe decision row to the paper trades CSV (non-blocking)."""
    fee_adj_profit = (1.0 - ask_price) * p_true - ask_price * (1.0 - p_true) if would_execute else 0.0
    row = {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "market_id": state.market_id,
        "condition_id": state.condition_id,
        "btc_price": f"{state.btc_price:.2f}",
        "opening_price": f"{state.opening_price:.2f}",
        "token_side": token_side,
        "token_id": token_id,
        "ask_price": f"{ask_price:.4f}",
        "P_true": f"{p_true:.4f}",
        "Z_score": f"{z_score:.4f}",
        "expected_value": f"{ev:.4f}",
        "would_execute": str(would_execute),
        "fee_adjusted_profit_if_right": f"{fee_adj_profit:.4f}",
        "actual_outcome": "PENDING",
        "actual_pnl": "",
    }
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, _write_decision_row, row)
    logger.info(
        "[PAPER] side=%s ask=%.4f P=%.3f Z=%.3f EV=%.4f execute=%s",
        token_side, ask_price, p_true, z_score, ev, would_execute,
    )


async def resolve_pending_trades() -> None:
    """
    Post-run: read all PENDING rows from the CSV, fetch resolved prices from
    Gamma API, and update actual_outcome / actual_pnl in-place.
    """
    path = _csv_path()
    if not os.path.exists(path):
        logger.info("No paper trades CSV found at %s", path)
        return

    with open(path, "r", newline="") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return

    pending = [r for r in rows if r.get("actual_outcome") == "PENDING"]
    if not pending:
        logger.info("No pending paper trades to resolve")
        return

    # Gather unique condition IDs
    condition_ids = list({r["condition_id"] for r in pending if r["condition_id"]})
    resolutions: dict[str, str] = {}  # condition_id → "UP" | "DOWN" | "UNKNOWN"

    async with aiohttp.ClientSession() as session:
        for cid in condition_ids:
            try:
                url = f"{GAMMA_API_BASE}/markets"
                async with session.get(
                    url,
                    params={"conditionId": cid},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        markets = data if isinstance(data, list) else data.get("markets", [])
                        for m in markets:
                            if m.get("conditionId") == cid or m.get("condition_id") == cid:
                                winner = _extract_winner(m)
                                resolutions[cid] = winner
                                break
            except Exception as exc:
                logger.warning("Could not resolve condition %s: %s", cid, exc)

    # Update rows
    for row in rows:
        if row.get("actual_outcome") != "PENDING":
            continue
        cid = row.get("condition_id", "")
        winner = resolutions.get(cid, "UNKNOWN")
        if winner == "UNKNOWN":
            continue
        side = row["token_side"]
        ask = float(row["ask_price"])
        correct = side == winner
        row["actual_outcome"] = "WIN" if correct else "LOSS"
        row["actual_pnl"] = f"{1.0 - ask:.4f}" if correct else f"{-ask:.4f}"

    # Write back
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    wins = sum(1 for r in rows if r.get("actual_outcome") == "WIN")
    losses = sum(1 for r in rows if r.get("actual_outcome") == "LOSS")
    logger.info("Paper trade resolution complete: %d WIN / %d LOSS", wins, losses)


def _extract_winner(market: dict) -> str:
    """
    Infer resolved side from market data.
    Gamma markets have a `winner` field or resolved token prices.
    """
    winner = market.get("winner") or market.get("winnerToken")
    if winner:
        return str(winner).upper()

    # Fallback: check if market is resolved and outcome token
    outcomes = market.get("outcomes", [])
    tokens = market.get("outcomePrices", [])
    for i, price in enumerate(tokens):
        try:
            if float(price) >= 0.99:
                return str(outcomes[i]).upper() if i < len(outcomes) else "UNKNOWN"
        except (ValueError, TypeError):
            pass

    return "UNKNOWN"
