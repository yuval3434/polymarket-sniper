"""
PnL Tracker — Live running profit/loss display
================================================
Maintains session and all-time statistics.
Prints a formatted live dashboard to stdout after every closed trade.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from core.database import log_performance

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    trade_id: int
    market_id: str
    token_side: str
    entry_price: float
    exit_price: float
    exit_type: str          # EXPIRY / TAKE_PROFIT / STOP_LOSS / DRY_RUN
    size_usdc: float
    size_shares: float
    pnl_usd: float
    pnl_pct: float
    win: bool
    timestamp: str = field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())


@dataclass
class SessionStats:
    trades: list[TradeRecord] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.trades)

    @property
    def wins(self) -> int:
        return sum(1 for t in self.trades if t.win)

    @property
    def losses(self) -> int:
        return sum(1 for t in self.trades if not t.win)

    @property
    def win_rate(self) -> float:
        return self.wins / self.total if self.total > 0 else 0.0

    @property
    def total_pnl_usd(self) -> float:
        return sum(t.pnl_usd for t in self.trades)

    @property
    def avg_pnl_pct(self) -> float:
        if not self.trades:
            return 0.0
        return sum(t.pnl_pct for t in self.trades) / len(self.trades)

    @property
    def max_drawdown(self) -> float:
        """Maximum peak-to-trough drawdown over all trades."""
        if not self.trades:
            return 0.0
        peak = 0.0
        running = 0.0
        worst_dd = 0.0
        for t in self.trades:
            running += t.pnl_usd
            peak = max(peak, running)
            dd = peak - running
            worst_dd = max(worst_dd, dd)
        return worst_dd

    @property
    def current_streak(self) -> tuple[str, int]:
        """Returns ('W'|'L', streak_count) for the current consecutive run."""
        if not self.trades:
            return ("W", 0)
        kind = "W" if self.trades[-1].win else "L"
        count = 0
        for t in reversed(self.trades):
            if (t.win and kind == "W") or (not t.win and kind == "L"):
                count += 1
            else:
                break
        return (kind, count)

    def last_n_win_rate(self, n: int = 10) -> float:
        recent = self.trades[-n:]
        if not recent:
            return 0.0
        return sum(1 for t in recent if t.win) / len(recent)


class PnLTracker:
    def __init__(self, strategy_version: int = 1) -> None:
        self.session = SessionStats()
        self.strategy_version = strategy_version
        self._perf_log_interval = 10   # log to DB every N trades

    def record(self, record: TradeRecord) -> None:
        self.session.trades.append(record)
        self._print_trade(record)
        self._print_dashboard()

        if self.session.total % self._perf_log_interval == 0:
            # Fire-and-forget into thread pool — must not block the event loop.
            try:
                loop = asyncio.get_running_loop()
                loop.run_in_executor(
                    None, log_performance,
                    self.session.total, self.session.win_rate,
                    self.session.total_pnl_usd, self.session.avg_pnl_pct,
                    self.session.max_drawdown, self.strategy_version,
                )
            except RuntimeError:
                log_performance(
                    total_trades=self.session.total,
                    win_rate=self.session.win_rate,
                    total_pnl_usd=self.session.total_pnl_usd,
                    avg_pnl_pct=self.session.avg_pnl_pct,
                    max_drawdown=self.session.max_drawdown,
                    strategy_version=self.strategy_version,
                )

    def _print_trade(self, t: TradeRecord) -> None:
        direction = "▲ UP  " if t.token_side == "UP" else "▼ DOWN"
        pnl_sign  = "+" if t.pnl_usd >= 0 else ""
        exit_tag  = {
            "TAKE_PROFIT": "TAKE PROFIT",
            "STOP_LOSS":   "STOP LOSS",
            "EXPIRY":      "EXPIRY",
            "DRY_RUN":     "PAPER",
        }.get(t.exit_type, t.exit_type)

        if t.win:
            result_line = f"  RESULT  : *** WIN ***   +${t.pnl_usd:.4f} USDC  ({pnl_sign}{t.pnl_pct:.1f}%)"
            border      = "═" * 62
        else:
            result_line = f"  RESULT  : --- LOSS ---  -${abs(t.pnl_usd):.4f} USDC  ({t.pnl_pct:.1f}%)"
            border      = "─" * 62

        print(
            f"\n{border}\n"
            f"  POSITION CLOSED  {direction}  [{exit_tag}]  Trade #{self.session.total}\n"
            f"  Entry   : ${t.entry_price:.4f}  →  Exit: ${t.exit_price:.4f}"
            f"   Size: ${t.size_usdc:.2f}\n"
            f"{result_line}\n"
            f"{border}"
        )

    def _print_dashboard(self) -> None:
        s = self.session
        streak_kind, streak_n = s.current_streak
        streak_str = f"{streak_n}×{'W' if streak_kind=='W' else 'L'}"
        pnl_sign   = "+" if s.total_pnl_usd >= 0 else ""
        bar_w, bar_l = int(s.win_rate * 20), 20 - int(s.win_rate * 20)

        print(
            f"  ┌─ SESSION DASHBOARD {'─'*39}┐\n"
            f"  │  Trades : {s.total:>4}   W/L : {s.wins}/{s.losses}   "
            f"Streak : {streak_str:<6}          │\n"
            f"  │  Win Rate : {s.win_rate:>6.1%}   "
            f"[{'█'*bar_w}{'░'*bar_l}]             │\n"
            f"  │  Total PnL : {pnl_sign}${s.total_pnl_usd:>8.4f} USDC   "
            f"Avg/trade : {pnl_sign}{s.avg_pnl_pct:.2f}%  │\n"
            f"  │  Max Drawdown : ${s.max_drawdown:.4f}   "
            f"Last-10 WR : {s.last_n_win_rate(10):.1%}          │\n"
            f"  └{'─'*60}┘"
        )
