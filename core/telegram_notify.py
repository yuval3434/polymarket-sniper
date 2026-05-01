"""
Telegram Notifier — Trade alerts via Telegram Bot API
======================================================
Sends a message on:
  1. BET PLACED  — when a position opens
  2. TRADE CLOSED — when a position resolves (win/loss + session PnL)

Setup:
  1. Create a bot via @BotFather on Telegram → get TELEGRAM_BOT_TOKEN
  2. Send any message to your bot, then run:
       curl "https://api.telegram.org/bot<TOKEN>/getUpdates"
     to find your TELEGRAM_CHAT_ID
  3. Add both to .env

If either env var is missing the notifier silently does nothing.
"""
from __future__ import annotations

import asyncio
import logging
import os

import aiohttp

logger = logging.getLogger(__name__)

_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
_API_URL = f"https://api.telegram.org/bot{_TOKEN}/sendMessage"
_TIMEOUT = aiohttp.ClientTimeout(total=5)

_enabled = bool(_TOKEN and _CHAT_ID)


def _is_enabled() -> bool:
    return bool(os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"))


async def _send(text: str) -> None:
    """Fire-and-forget — never raises, never blocks the caller."""
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            await session.post(url, json={
                "chat_id":    chat_id,
                "text":       text,
                "parse_mode": "HTML",
            })
    except Exception as exc:
        logger.debug("Telegram send failed (non-critical): %s", exc)


def notify_bet_placed(
    token_side: str,
    market_id: str,
    entry_price: float,
    size_usdc: float,
    p_true: float,
    z_score: float,
    stop_price: float,
    is_dry_run: bool,
) -> None:
    """Call from sync or async context — schedules fire-and-forget."""
    if not _is_enabled():
        return

    mode  = "📄 PAPER" if is_dry_run else "🔴 LIVE"
    arrow = "📈" if token_side == "UP" else "📉"
    text  = (
        f"{arrow} <b>BET PLACED</b> [{mode}]\n"
        f"Side    : <b>{token_side}</b>\n"
        f"Market  : {market_id}\n"
        f"Entry   : <code>${entry_price:.4f}</code>   Size: <code>${size_usdc:.2f}</code>\n"
        f"P(win)  : <code>{p_true:.1%}</code>   Z: <code>{z_score:.3f}</code>\n"
        f"Stop    : <code>${stop_price:.4f}</code> (−50%)"
    )
    _fire(text)


def notify_trade_closed(
    token_side: str,
    exit_type: str,
    entry_price: float,
    exit_price: float,
    pnl_usd: float,
    pnl_pct: float,
    rr: float,
    size_usdc: float,
    win: bool,
    session_wins: int,
    session_losses: int,
    session_pnl: float,
    is_dry_run: bool,
) -> None:
    """Call from sync or async context — schedules fire-and-forget."""
    if not _is_enabled():
        return

    mode      = "📄 PAPER" if is_dry_run else "🔴 LIVE"
    outcome   = "✅ <b>WIN</b>" if win else "❌ <b>LOSS</b>"
    pnl_sign  = "+" if pnl_usd >= 0 else ""
    sess_sign = "+" if session_pnl >= 0 else ""
    exit_map  = {
        "EXPIRY":          "⏰ Expiry",
        "STOP_LOSS":       "🛑 Stop Loss",
        "TRAIL_CONTINUOUS":"📉 Trail",
        "TRAIL_LOCKED":    "🔒 Trail Locked",
    }
    exit_label = exit_map.get(exit_type, exit_type)

    text = (
        f"{outcome} [{mode}]  {exit_label}\n"
        f"Side    : <b>{token_side}</b>\n"
        f"Entry   : <code>${entry_price:.4f}</code> → Exit: <code>${exit_price:.4f}</code>\n"
        f"P&amp;L     : <code>{pnl_sign}${pnl_usd:.2f}</code>  ({pnl_sign}{pnl_pct:.1f}%)\n"
        f"R:R     : <code>{rr:+.2f}R</code>\n"
        f"Size    : <code>${size_usdc:.2f}</code>\n"
        f"─────────────────────\n"
        f"Session : {session_wins}W / {session_losses}L  "
        f"PnL: <code>{sess_sign}${session_pnl:.2f}</code>"
    )
    _fire(text)


def _fire(text: str) -> None:
    """Schedule the send coroutine on the running event loop (fire-and-forget)."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_send(text))
    except RuntimeError:
        # No running loop — skip silently (shouldn't happen in production)
        pass
