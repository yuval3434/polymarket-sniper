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
    async_resolve_shadow_trades_for_market,
)
from core.pnl_tracker import PnLTracker, TradeRecord
from core.risk_manager import RiskManager
from core.tracker import TradeEntry, record_trade as _tracker_record
import engine.strategy_loader as _scfg
from market.state import MarketState

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

    def correct_entry_price(
        self,
        actual_price: float,
        actual_shares: float | None = None,
    ) -> None:
        """
        Correct entry price to actual fill price and recalculate all derived fields.
        Pass actual_shares from on-chain balance when available — do NOT back-calculate
        from size_usdc because a partial IOC fill means we own fewer shares than intended.
        """
        old_price  = self.entry_price
        old_shares = self.size_shares

        self.entry_price = actual_price
        self.trail_level = 0

        if actual_shares is not None:
            # On-chain verified — use ground-truth share count
            self.size_shares = actual_shares
            self.size_usdc   = round(actual_shares * actual_price, 4)
        else:
            # Fallback: back-calculate (assumes full fill at actual_price)
            self.size_shares = self.size_usdc / actual_price

        self.trailing_stop_price    = max(actual_price * INITIAL_STOP_MULT, 0.01)
        self.peak_bid               = actual_price
        self.initial_risk_per_share = actual_price - self.trailing_stop_price

        slippage_pct = ((actual_price - old_price) / old_price) * 100 if old_price > 0 else 0.0
        fill_note    = f"  Filled USDC : ${self.size_usdc:.2f}\n" if actual_shares is not None else ""

        if abs(slippage_pct) >= 5.0:
            logger.warning(
                "HIGH SLIPPAGE DETECTED — Expected: $%.4f  Actual Fill: $%.4f  "
                "Slippage: %+.1f%%  |  shares %.4f → %.4f  stop $%.4f",
                old_price, actual_price, slippage_pct,
                old_shares, self.size_shares, self.trailing_stop_price,
            )
        else:
            logger.info(
                "Entry corrected: signal=$%.4f → actual_fill=$%.4f (%+.1f%%) | "
                "shares %.4f → %.4f | stop=$%.4f",
                old_price, actual_price, slippage_pct,
                old_shares, self.size_shares, self.trailing_stop_price,
            )

        print(
            f"\n  ── FILL CONFIRMED ──────────────────────────────────────\n"
            f"  Signal price : ${old_price:.4f}  ({old_shares:.4f} shares)\n"
            f"  Actual fill  : ${actual_price:.4f}  ({self.size_shares:.4f} shares)\n"
            f"{fill_note}"
            f"  Slippage     : {slippage_pct:+.1f}%"
            + ("  ⚠ HIGH SLIPPAGE" if abs(slippage_pct) >= 5.0 else "") + "\n"
            f"  New stop     : ${self.trailing_stop_price:.4f}\n"
            f"  ────────────────────────────────────────────────────────\n"
        )

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
    ) -> None:
        self.pnl_tracker      = pnl_tracker
        self.risk_manager     = risk_manager
        self.outcome_queue    = outcome_queue
        self.strategy_version = strategy_version
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
        pending_note = "  ⏳ Awaiting fill confirmation — entry will update\n" if not is_dry_run else ""
        print(
            f"\n{'═'*62}\n"
            f"  BET PLACED  {arrow} {token_side}  {mode}\n"
            f"  Market  : {market_id}\n"
            f"  Entry   : ${entry_price:.4f} (signal)   Size: ${size_usdc:.2f} USDC"
            f"  ({size_shares:.4f} shares)\n"
            f"  P(win)  : {p_true:.1%}   Z-score: {z_score:.3f}\n"
            f"  Stop    : ${pos.trailing_stop_price:.4f}  (−50% / Level 0)\n"
            f"  Trail   : +25% → breakeven | +50% → lock +25% floor\n"
            f"{pending_note}"
            f"{'═'*62}\n"
        )

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
            in_hte_window = (bid > HTE_BID_THRESHOLD and tau < HTE_TAU_THRESHOLD)
            if in_hte_window and bid > pos.trailing_stop_price:
                logger.debug(
                    "HTE: holding to expiry | bid=%.4f tau=%.0fs stop=%.4f",
                    bid, tau, pos.trailing_stop_price,
                )
                continue

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
        actual_exit_price = exit_bid

        if not pos.is_dry_run and self.order_manager is not None:
            logger.info(
                "Initiating smart exit [%s]: token=%s shares=%.4f bid=%.4f",
                exit_type, pos.token_id, pos.size_shares, exit_bid,
            )
            success, actual_exit_price = await self.order_manager.smart_exit(
                token_id=pos.token_id,
                size_shares=pos.size_shares,
                bid_price=exit_bid,
                exit_type=exit_type,
            )

            if not success:
                halt_msg = (
                    f"SELL_EXHAUSTED [{exit_type}]: token={pos.token_id} "
                    f"size={pos.size_shares:.4f} bid={exit_bid:.4f} — "
                    "Manual intervention required!"
                )
                logger.critical("CRITICAL_ERROR: %s", halt_msg)
                self.risk_manager.force_halt(halt_msg)
                return  # keep _position open for next monitor tick

        pnl_usd = (actual_exit_price - pos.entry_price) * pos.size_shares
        pnl_pct = ((actual_exit_price / pos.entry_price) - 1.0) * 100
        win     = pnl_usd > 0
        await self._finalise(pos, actual_exit_price, exit_type, pnl_usd, pnl_pct, win)

    async def _close_at_expiry(self, pos: OpenPosition, state: MarketState) -> None:
        bid = pos.current_bid(state)
        win = bid >= 0.50
        exit_price = 1.0 if win else 0.0
        pnl_usd = (exit_price - pos.entry_price) * pos.size_shares
        pnl_pct = ((exit_price / pos.entry_price) - 1.0) * 100
        await self._finalise(pos, exit_price, "EXPIRY", pnl_usd, pnl_pct, win)

        # Resolve shadow trades for this market so the evaluator can learn from them
        market_winner = pos.token_side if win else ("DOWN" if pos.token_side == "UP" else "UP")
        n = await async_resolve_shadow_trades_for_market(pos.market_id, market_winner)
        if n:
            logger.info("[SHADOW] Resolved %d shadow trade(s) for market %s — winner=%s", n, pos.market_id, market_winner)

        # Queue claim — retried every 2min until oracle resolves (can take 5-30min)
        if win and not pos.is_dry_run and self.order_manager is not None:
            self.order_manager.add_pending_claim(pos.condition_id, pos.token_side)

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
            f"  Size   : ${pos.size_usdc:.2f} USDC  ({pos.size_shares:.4f} shares)\n"
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

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def has_open_position(self) -> bool:
        return self._position is not None

    @property
    def is_halted(self) -> bool:
        return self.risk_manager.halted
