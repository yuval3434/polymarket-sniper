"""
Position Manager Agent — Trailing Stop-Loss System
===================================================
Exit logic overview:

  INITIAL STATE
  ─────────────
  Entry price = P  (e.g. $0.60/share)
  Initial stop = P × 0.50  (e.g. $0.30  →  −50% loss cap)
  No hard take-profit: winning trades are held open.

  TRAILING STOP STATE MACHINE  (trail_level)
  ───────────────────────────────────────────
  Level 0 — Initial stop at P × 0.50
    • Bid rises to P × 1.25 (+25%)  →  advance to Level 1

  Level 1 — Stop moves to BREAKEVEN (P × 1.00)
    • Trade is now "risk-free": worst case is 0% loss
    • Bid rises to P × 1.50 (+50%)  →  advance to Level 2

  Level 2 — Stop moves to +25% profit (P × 1.25)
    • Guaranteed minimum profit of +25% if stopped out
    • Upside remains unlimited until expiry

  At any level: if bid drops to or below trailing_stop_price → TRAILING_STOP exit.

  HOLD-TO-EXPIRY OVERRIDE
  ────────────────────────
  If the position is deeply in the money (bid > 0.85) with < 60s on the clock,
  the trailing stop is suspended. The bot consciously waits for the full $1.00
  expiry payout rather than selling at $0.87 for a smaller gain.
  Exception: if bid drops below the trailing stop even during HTE window, we exit.

  RISK/REWARD CALCULATION
  ───────────────────────
  Initial risk per share = P − (P × 0.50) = P × 0.50
  Realized reward        = exit_price − P
  R:R ratio              = reward / initial_risk
    > 1.0  →  made more than was risked  (target)
    = 0.0  →  breakeven exit
    < 0.0  →  stop-loss (negative, capped at −1.0 on full stop-out)
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from core.database import (
    async_log_decision,
    async_log_trade,
    async_update_decision_outcome,
)
from core.pnl_tracker import PnLTracker, TradeRecord
from core.risk_manager import RiskManager
from core.tracker import TradeEntry, record_trade as _tracker_record
import engine.strategy_loader as _scfg
from engine.fee_filter import _taker_fee_per_share
from market.state import MarketState
from monitor.telegram_notifier import TelegramNotifier

logger = logging.getLogger(__name__)

# ── Trailing stop parameters ───────────────────────────────────────────────
INITIAL_STOP_MULT   = 0.50   # initial stop = entry × 0.50  (−50% loss)
TRAIL_L1_TRIGGER    = 1.25   # +25% profit  → stop moves to breakeven
TRAIL_L2_TRIGGER    = 1.50   # +50% profit  → stop moves to +25%
TRAIL_L1_STOP_MULT  = 1.00   # breakeven stop
TRAIL_L2_STOP_MULT  = 1.25   # +25% guaranteed floor

# ── Hold-to-expiry parameters ──────────────────────────────────────────────
HTE_BID_THRESHOLD   = 0.85   # bid must be above this to trigger HTE
HTE_TAU_THRESHOLD   = 60.0   # seconds remaining when HTE kicks in

MONITOR_INTERVAL_S  = 0.5    # position check every 500ms
MIN_HOLD_SECONDS    = 5.0    # ignore exits within 5s of entry (noise)


@dataclass
class OpenPosition:
    market_id:      str
    condition_id:   str
    token_side:     str       # "UP" or "DOWN"
    token_id:       str
    entry_price:    float
    size_usdc:      float
    size_shares:    float
    entry_time:     float = field(default_factory=time.monotonic)
    entry_ts_iso:   str   = field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())
    decision_id:    int   = 0
    p_true:         float = 0.5
    z_score:        float = 0.0
    is_dry_run:     bool  = True

    # ── Trailing stop state (populated in __post_init__) ──────────────────
    # trail_level tracks which ratchet tier we're on:
    #   0 = initial stop (entry × 0.50)
    #   1 = breakeven stop (entry × 1.00) — triggered at +25%
    #   2 = +25% floor stop (entry × 1.25) — triggered at +50%
    trail_level:         int   = field(init=False)
    trailing_stop_price: float = field(init=False)
    peak_bid:            float = field(init=False)
    initial_risk_per_share: float = field(init=False)

    def __post_init__(self) -> None:
        # Initial stop = 50% of entry price (e.g. entry=0.60 → stop=0.30)
        self.trailing_stop_price    = max(self.entry_price * INITIAL_STOP_MULT, 0.01)
        self.peak_bid               = self.entry_price
        self.trail_level            = 0
        # Locked in at open for R:R calculation — never changes
        self.initial_risk_per_share = self.entry_price - self.trailing_stop_price

    def age_seconds(self) -> float:
        return time.monotonic() - self.entry_time

    def current_bid(self, state: MarketState) -> float:
        return state.up_best_bid if self.token_side == "UP" else state.down_best_bid

    def update_trail(self, bid: float) -> bool:
        """
        Ratchet the trailing stop upward as the trade moves in our favour.
        Returns True if the stop level was advanced (for logging).

        Ratchet math:
          Level 0→1: bid reaches entry × 1.25  →  stop = entry × 1.00
            e.g. entry=0.60, bid=0.75  →  stop moves from 0.30 to 0.60
            Now worst-case is 0% loss instead of −50%.

          Level 1→2: bid reaches entry × 1.50  →  stop = entry × 1.25
            e.g. entry=0.60, bid=0.90  →  stop moves from 0.60 to 0.75
            Now minimum locked-in profit is +25%.
        """
        self.peak_bid = max(self.peak_bid, bid)
        advanced = False

        if self.trail_level < 2 and self.peak_bid >= self.entry_price * TRAIL_L2_TRIGGER:
            # +50% peak reached → lock in +25% floor
            new_stop = self.entry_price * TRAIL_L2_STOP_MULT
            if new_stop > self.trailing_stop_price:
                self.trailing_stop_price = new_stop
                self.trail_level = 2
                advanced = True

        elif self.trail_level < 1 and self.peak_bid >= self.entry_price * TRAIL_L1_TRIGGER:
            # +25% peak reached → move stop to breakeven
            new_stop = self.entry_price * TRAIL_L1_STOP_MULT
            if new_stop > self.trailing_stop_price:
                self.trailing_stop_price = new_stop
                self.trail_level = 1
                advanced = True

        return advanced

    def risk_reward_ratio(self, exit_price: float) -> float:
        """
        Realized R:R = (exit − entry) / initial_risk_per_share
          > 0  →  profitable exit
          = 0  →  breakeven
          < 0  →  loss (minimum −1.0 on full stop-out)
        """
        if self.initial_risk_per_share <= 0:
            return 0.0
        return (exit_price - self.entry_price) / self.initial_risk_per_share


class PositionManager:
    def __init__(
        self,
        pnl_tracker: PnLTracker,
        risk_manager: RiskManager,
        outcome_queue: asyncio.Queue,
        strategy_version: int = 1,
        notifier: Optional[TelegramNotifier] = None,
    ) -> None:
        self.pnl_tracker      = pnl_tracker
        self.risk_manager     = risk_manager
        self.outcome_queue    = outcome_queue
        self.strategy_version = strategy_version
        self.notifier         = notifier
        self._position: Optional[OpenPosition] = None
        self.order_manager    = None  # wired by main.py after both are created

    # ------------------------------------------------------------------
    # Position lifecycle
    # ------------------------------------------------------------------

    def open_position(
        self,
        market_id: str,
        condition_id: str,
        token_side: str,
        token_id: str,
        entry_price: float,
        p_true: float,
        z_score: float,
        size_usdc: float,
        is_dry_run: bool = True,
    ) -> None:
        if self._position is not None:
            logger.warning("Overwriting existing position — should not happen")
            return

        size_shares = size_usdc / entry_price
        asyncio.create_task(async_log_decision(
            agent="POSITION_MANAGER",
            decision_type="SNIPE",
            details={
                "market_id": market_id,
                "token_side": token_side,
                "entry_price": entry_price,
                "size_usdc": size_usdc,
                "p_true": p_true,
                "z_score": z_score,
                "dry_run": is_dry_run,
            },
        ))
        dec_id = 0
        self._position = OpenPosition(
            market_id=market_id,
            condition_id=condition_id,
            token_side=token_side,
            token_id=token_id,
            entry_price=entry_price,
            size_usdc=size_usdc,
            size_shares=size_shares,
            decision_id=dec_id,
            p_true=p_true,
            z_score=z_score,
            is_dry_run=is_dry_run,
        )
        pos = self._position
        arrow = "▲" if token_side == "UP" else "▼"
        mode  = "[PAPER]" if is_dry_run else "[LIVE]"
        logger.info(
            "Position opened: %s %s @ %.4f | $%.2f | P=%.3f | Z=%.3f | stop=%.4f%s",
            token_side, market_id, entry_price, size_usdc, p_true, z_score,
            pos.trailing_stop_price, " [DRY RUN]" if is_dry_run else " [LIVE]",
        )
        print(
            f"\n{'═'*62}\n"
            f"  BET PLACED  {arrow} {token_side}  {mode}\n"
            f"  Market  : {market_id}\n"
            f"  Entry   : ${entry_price:.4f}   Size: ${size_usdc:.2f} pUSD"
            f"  ({size_shares:.4f} shares)\n"
            f"  P(win)  : {p_true:.1%}   Z-score: {z_score:.3f}\n"
            f"  Stop    : ${pos.trailing_stop_price:.4f}  (−50% / Level 0)\n"
            f"  Trail   : +25% → breakeven | +50% → lock +25% floor\n"
            f"{'═'*62}\n"
        )
        if self.notifier:
            self.notifier.notify_trade_open(
                token_side=token_side,
                market_id=market_id,
                entry_price=entry_price,
                size_usdc=size_usdc,
                p_true=p_true,
                z_score=z_score,
                is_dry_run=is_dry_run,
            )

    def update_entry_price(self, actual_price: float, actual_size: float) -> None:
        """Sync the internal position trackers to the actual filled price from the API."""
        if self._position is None:
            return
            
        old_price = self._position.entry_price
        self._position.entry_price = actual_price
        
        if actual_size > 0:
            self._position.size_shares = actual_size
            self._position.size_usdc = actual_price * actual_size
            
        # Recalculate stop-loss parameters with the new realistic price
        self._position.trailing_stop_price = max(self._position.entry_price * INITIAL_STOP_MULT, 0.01)
        self._position.peak_bid = max(self._position.peak_bid, self._position.entry_price)
        self._position.initial_risk_per_share = self._position.entry_price - self._position.trailing_stop_price
        
        logger.info("Position entry synced to actual fill: $%.4f (was $%.4f)", actual_price, old_price)

    async def monitor_loop(self, state: MarketState) -> None:
        """
        Runs every 500ms. Implements the trailing stop state machine and
        hold-to-expiry logic. Never exits the loop — runs for the session lifetime.
        """
        while True:
            await asyncio.sleep(MONITOR_INTERVAL_S)

            if self._position is None:
                continue

            pos = self._position
            tau = state.seconds_to_expiry()
            bid = pos.current_bid(state)
            age = pos.age_seconds()

            # ── Expiry close ──────────────────────────────────────────────
            if tau <= 0:
                await self._close_at_expiry(pos, state)
                continue

            # ── Noise suppression: ignore exits immediately after entry ───
            if age < MIN_HOLD_SECONDS:
                continue

            # ── Advance trailing stop (ratchet up, never down) ────────────
            advanced = pos.update_trail(bid)
            if advanced:
                level_labels = {1: "BREAKEVEN", 2: "+25% FLOOR"}
                logger.info(
                    "TRAIL ADVANCED to Level %d (%s): stop=%.4f | bid=%.4f | entry=%.4f",
                    pos.trail_level, level_labels.get(pos.trail_level, "?"),
                    pos.trailing_stop_price, bid, pos.entry_price,
                )

            # ── Hold-to-Expiry override ───────────────────────────────────
            # If we're deeply in the money and close to expiry, suspend the
            # trailing stop and wait for the full $1.00 payout.
            # Exception: if bid drops BELOW the current trailing stop even
            # during HTE window, the trailing stop still fires (protects against
            # sudden volatility collapsing the price in the final seconds).
            in_hte_window = (bid > HTE_BID_THRESHOLD and tau < HTE_TAU_THRESHOLD)
            if in_hte_window and bid > pos.trailing_stop_price:
                logger.debug(
                    "HTE: holding to expiry | bid=%.4f tau=%.0fs stop=%.4f",
                    bid, tau, pos.trailing_stop_price,
                )
                continue  # skip trailing stop check — hold for full payout

            # ── Trailing Stop check ───────────────────────────────────────
            if bid <= pos.trailing_stop_price:
                exit_label = {
                    0: "STOP_LOSS",
                    1: "TRAIL_BREAKEVEN",
                    2: "TRAIL_PROFIT_FLOOR",
                }.get(pos.trail_level, "TRAILING_STOP")
                logger.info(
                    "%s: bid=%.4f <= stop=%.4f | level=%d | entry=%.4f",
                    exit_label, bid, pos.trailing_stop_price,
                    pos.trail_level, pos.entry_price,
                )
                await self._close_early(pos, bid, exit_label, state)
                continue

    # ------------------------------------------------------------------
    # Close helpers
    # ------------------------------------------------------------------

    async def _close_early(
        self,
        pos: OpenPosition,
        exit_bid: float,
        exit_type: str,
        state: MarketState,
    ) -> None:
        if not pos.is_dry_run and self.order_manager is not None:
            logger.info(
                "Placing live SELL [%s]: token=%s shares=%.4f bid=%.4f",
                exit_type, pos.token_id, pos.size_shares, exit_bid,
            )
            sell_result = await self.order_manager.sell(
                token_id=pos.token_id,
                size_shares=pos.size_shares,
                bid_price=exit_bid,
            )
            if sell_result is None:
                logger.error("SELL failed — keeping position open")
                return

        # Calculate net PnL by deducting Polymarket V2 dynamic taker fees
        entry_fee = _taker_fee_per_share(pos.entry_price, state.fee_rate_r, state.fee_rate_e)
        exit_fee  = _taker_fee_per_share(exit_bid, state.fee_rate_r, state.fee_rate_e)
        
        net_entry = pos.entry_price + entry_fee
        net_exit  = exit_bid - exit_fee

        pnl_usd = (net_exit - net_entry) * pos.size_shares
        pnl_pct = ((net_exit / net_entry) - 1.0) * 100
        win     = pnl_usd > 0
        await self._finalise(pos, exit_bid, exit_type, pnl_usd, pnl_pct, win)

    async def _close_at_expiry(self, pos: OpenPosition, state: MarketState) -> None:
        bid = pos.current_bid(state)
        win = bid >= 0.50
        exit_price = 1.0 if win else 0.0
        
        # Expiry resolution has no exit fee, only entry fee
        entry_fee = _taker_fee_per_share(pos.entry_price, state.fee_rate_r, state.fee_rate_e)
        net_entry = pos.entry_price + entry_fee
        
        pnl_usd = (exit_price - net_entry) * pos.size_shares
        pnl_pct = ((exit_price / net_entry) - 1.0) * 100
        await self._finalise(pos, exit_price, "EXPIRY", pnl_usd, pnl_pct, win)

    async def close_position_with_resolution(
        self,
        resolution: str,
        actual_exit_price: float,
    ) -> None:
        if self._position is None:
            return
        pos     = self._position
        win     = resolution == "WIN"
        exit_p  = actual_exit_price
        pnl_usd = (exit_p - pos.entry_price) * pos.size_shares
        pnl_pct = ((exit_p / pos.entry_price) - 1.0) * 100
        await self._finalise(pos, exit_p, "EXPIRY", pnl_usd, pnl_pct, win)

    async def _finalise(
        self,
        pos: OpenPosition,
        exit_price: float,
        exit_type: str,
        pnl_usd: float,
        pnl_pct: float,
        win: bool,
    ) -> None:
        self._position = None  # clear first — prevents re-entry

        # ── R:R calculation ───────────────────────────────────────────────
        rr = pos.risk_reward_ratio(exit_price)
        # Interpret: rr=1.0 means "made exactly as much as was risked"
        #            rr=2.0 means "made 2× the amount risked"
        #            rr=-1.0 means "full stop-out (lost 100% of risked capital)"
        rr_str = f"{rr:+.2f}R"

        trade_id = await async_log_trade(
            market_id=pos.market_id,
            condition_id=pos.condition_id,
            token_side=pos.token_side,
            token_id=pos.token_id,
            entry_price=pos.entry_price,
            entry_size_usdc=pos.size_usdc,
            size_shares=pos.size_shares,
            exit_price=exit_price,
            exit_type=exit_type,
            pnl_usd=pnl_usd,
            pnl_pct=pnl_pct,
            win=win,
            strategy_version=self.strategy_version,
            dry_run=pos.is_dry_run,
        )

        if pos.decision_id:
            await async_update_decision_outcome(
                pos.decision_id,
                f"{'WIN' if win else 'LOSS'} via {exit_type} exit={exit_price:.4f} rr={rr:.2f}R",
            )

        record = TradeRecord(
            trade_id=trade_id,
            market_id=pos.market_id,
            token_side=pos.token_side,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            exit_type=exit_type,
            size_usdc=pos.size_usdc,
            size_shares=pos.size_shares,
            pnl_usd=pnl_usd,
            pnl_pct=pnl_pct,
            win=win,
        )
        self.pnl_tracker.record(record)
        self.risk_manager.record_trade_result(pnl_usd, win)

        try:
            _tracker_record(TradeEntry(
                trade_id=trade_id,
                entry_timestamp=pos.entry_ts_iso,
                exit_timestamp=datetime.now(tz=timezone.utc).isoformat(),
                token_side=pos.token_side,
                entry_price=pos.entry_price,
                exit_price=exit_price,
                size_usdc=pos.size_usdc,
                pnl_usd=pnl_usd,
                pnl_pct=pnl_pct,
                outcome="WIN" if win else "LOSS",
                strategy_name=_scfg.strategy_name(),
                exit_reason=exit_type,
                market_id=pos.market_id,
                dry_run=pos.is_dry_run,
            ))
        except Exception as _exc:
            logger.warning("Tracker write failed: %s", _exc)

        # ── Trade close summary ───────────────────────────────────────────
        outcome_icon = "WIN" if win else "LOSS"
        print(
            f"\n{'═'*62}\n"
            f"  TRADE CLOSED  [{exit_type}]  →  {outcome_icon}\n"
            f"  Entry  : ${pos.entry_price:.4f}   Exit: ${exit_price:.4f}\n"
            f"  P&L    : ${pnl_usd:+.4f}  ({pnl_pct:+.1f}%)\n"
            f"  R:R    : {rr_str}  (trail level reached: {pos.trail_level})\n"
            f"  Size   : ${pos.size_usdc:.2f} pUSD  ({pos.size_shares:.4f} shares)\n"
            f"{'═'*62}\n"
        )
        logger.info(
            "CLOSED [%s] %s %s | entry=%.4f exit=%.4f | PnL=$%.4f (%.1f%%) | R:R=%s | trail_lvl=%d",
            exit_type, pos.token_side, pos.market_id,
            pos.entry_price, exit_price, pnl_usd, pnl_pct, rr_str, pos.trail_level,
        )

        try:
            self.outcome_queue.put_nowait({
                "trade_id":   trade_id,
                "pos":        pos,
                "exit_price": exit_price,
                "exit_type":  exit_type,
                "pnl_usd":    pnl_usd,
                "pnl_pct":    pnl_pct,
                "win":        win,
                "rr":         rr,
            })
        except asyncio.QueueFull:
            pass

        if self.notifier:
            self.notifier.notify_trade_close(
                token_side=pos.token_side,
                market_id=pos.market_id,
                entry_price=pos.entry_price,
                exit_price=exit_price,
                exit_type=exit_type,
                pnl_usd=pnl_usd,
                pnl_pct=pnl_pct,
                win=win,
                rr=rr,
                is_dry_run=pos.is_dry_run,
            )
            s = self.pnl_tracker.session
            streak_kind, streak_n = s.current_streak
            self.notifier.notify_session(
                total=s.total,
                wins=s.wins,
                losses=s.losses,
                win_rate=s.win_rate,
                total_pnl_usd=s.total_pnl_usd,
                last10_win_rate=s.last_n_win_rate(10),
                streak_kind=streak_kind,
                streak_n=streak_n,
            )

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def cancel_position(self) -> None:
        """Clear a just-opened position because the order failed to fill.
        Does NOT record a trade or send any notification."""
        if self._position is not None:
            logger.warning(
                "Cancelling unexecuted position: %s %s @ %.4f",
                self._position.token_side,
                self._position.market_id,
                self._position.entry_price,
            )
            self._position = None

    @property
    def has_open_position(self) -> bool:
        return self._position is not None

    @property
    def is_halted(self) -> bool:
        return self.risk_manager.halted
