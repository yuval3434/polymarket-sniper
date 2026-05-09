"""
Telegram Notifier
=================
Fire-and-forget async Telegram messages for trade events and session stats.
All failures are logged and swallowed — Telegram is non-critical infrastructure.

Required env vars:
    TELEGRAM_BOT_TOKEN  — from @BotFather
    TELEGRAM_CHAT_ID    — your chat/group ID (run /start with the bot to get it)

If either var is missing the notifier silently becomes a no-op.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

_TIMEOUT = aiohttp.ClientTimeout(total=8)


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str) -> None:
        self._enabled = bool(token and chat_id)
        self._url     = f"https://api.telegram.org/bot{token}/sendMessage"
        self._chat_id = chat_id
        if not self._enabled:
            logger.info("TelegramNotifier disabled — TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set")

    # ------------------------------------------------------------------
    # Public helpers — called by position_manager / pnl_tracker
    # ------------------------------------------------------------------

    def notify_trade_open(
        self,
        token_side: str,
        market_id: str,
        entry_price: float,
        size_usdc: float,
        p_true: float,
        z_score: float,
        is_dry_run: bool,
    ) -> None:
        if not self._enabled:
            return
        arrow  = "▲" if token_side == "UP" else "▼"
        mode   = "PAPER" if is_dry_run else "LIVE"
        msg = (
            f"🎯 SNIPE [{mode}]\n"
            f"{arrow} {token_side}  ·  {market_id}\n"
            f"Entry : ${entry_price:.4f}  ·  Size: ${size_usdc:.2f}\n"
            f"P(win): {p_true:.1%}  ·  Z: {z_score:.3f}"
        )
        asyncio.create_task(self._send(msg))

    def notify_trade_close(
        self,
        token_side: str,
        market_id: str,
        entry_price: float,
        exit_price: float,
        exit_type: str,
        pnl_usd: float,
        pnl_pct: float,
        win: bool,
        rr: float,
        is_dry_run: bool,
    ) -> None:
        if not self._enabled:
            return
        arrow  = "▲" if token_side == "UP" else "▼"
        mode   = "PAPER" if is_dry_run else "LIVE"
        icon   = "✅ WIN" if win else "❌ LOSS"
        pnl_s  = f"+${pnl_usd:.4f}" if pnl_usd >= 0 else f"-${abs(pnl_usd):.4f}"
        pct_s  = f"{pnl_pct:+.1f}%"
        msg = (
            f"{icon} — {exit_type} [{mode}]\n"
            f"{arrow} {token_side}  ·  {market_id}\n"
            f"${entry_price:.4f} → ${exit_price:.4f}  ({pct_s})\n"
            f"PnL: {pnl_s}  ·  R:R: {rr:+.2f}R"
        )
        asyncio.create_task(self._send(msg))

    def notify_session(
        self,
        total: int,
        wins: int,
        losses: int,
        win_rate: float,
        total_pnl_usd: float,
        last10_win_rate: float,
        streak_kind: str,
        streak_n: int,
    ) -> None:
        if not self._enabled:
            return
        pnl_s      = f"+${total_pnl_usd:.4f}" if total_pnl_usd >= 0 else f"-${abs(total_pnl_usd):.4f}"
        streak_s   = f"{streak_n}×{'W' if streak_kind == 'W' else 'L'}"
        msg = (
            f"📊 Session  #{total} trades\n"
            f"W/L: {wins}/{losses}  ·  WR: {win_rate:.1%}  ·  Streak: {streak_s}\n"
            f"PnL: {pnl_s}  ·  Last-10 WR: {last10_win_rate:.1%}"
        )
        asyncio.create_task(self._send(msg))

    # ------------------------------------------------------------------
    # Internal sender — never raises
    # ------------------------------------------------------------------

    async def _send(self, text: str) -> None:
        try:
            async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
                payload = {"chat_id": self._chat_id, "text": text}
                async with session.post(self._url, json=payload) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning("Telegram returned %s: %s", resp.status, body[:200])
        except Exception as exc:
            logger.warning("Telegram send failed: %s", exc)
