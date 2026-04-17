"""
Position Manager Agent — Active Trade Management
=================================================
After a snipe fires, this agent watches the open position every 500ms
and decides whether to:
  - HOLD  : stay until expiry (default)
  - TP    : take profit early if current bid > take_profit_threshold
  - SL    : stop loss if current bid < stop_loss_threshold

Mathematics:
  Entry price  = P   (e.g. 0.62)
  Current bid  = B   (what the market will pay NOW for our shares)

  Hold value (expected) = P_true * 1.00 + (1-P_true) * 0.00 = P_true
  Sell now value        = B  (certain)

  → Sell early if B > P_true  (lock in certain profit > expected)
  → Cut loss  if B < stop_loss_multiplier * P  (accept small loss before bigger one)

All decisions and outcomes are written to SQLite and the PnL tracker.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from core.database import log_decision, log_trade, update_decision_outcome
from core.pnl_tracker import PnLTracker, TradeRecord
from core.risk_manager import RiskManager
from core.tracker import TradeEntry, record_trade as _tracker_record
import engine.strategy_loader as _scfg
from market.state import MarketState

logger = logging.getLogger(__name__)

# Early exit thresholds (can be overridden via strategy_config.json)
TAKE_PROFIT_MULTIPLIER  = 1.25   # sell if bid > entry * 1.25 (25% gain on share price)
STOP_LOSS_MULTIPLIER    = 0.45   # sell if bid < entry * 0.45 (55% loss on share price)
MONITOR_INTERVAL_S      = 0.5    # check position every 500ms
MIN_HOLD_SECONDS        = 5.0    # never exit within 5s of entry (avoid noise)


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
    decision_id:    int = 0
    p_true:         float = 0.5
    z_score:        float = 0.0
    is_dry_run:     bool = True

    def age_seconds(self) -> float:
        return time.monotonic() - self.entry_time

    def current_bid(self, state: MarketState) -> float:
        if self.token_side == "UP":
            return state.up_best_bid
        return state.down_best_bid

    def take_profit_threshold(self) -> float:
        return min(self.entry_price * TAKE_PROFIT_MULTIPLIER, 0.98)

    def stop_loss_threshold(self) -> float:
        return max(self.entry_price * STOP_LOSS_MULTIPLIER, 0.01)


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
        self.outcome_queue    = outcome_queue  # sends closed TradeRecord to Evaluator
        self.strategy_version = strategy_version
        self._position: Optional[OpenPosition] = None

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
        dec_id = log_decision(
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
        )
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
        logger.info(
            "Position opened: %s %s @ %.4f | $%.2f | P=%.3f | Z=%.3f%s",
            token_side, market_id, entry_price, size_usdc, p_true, z_score,
            " [DRY RUN]" if is_dry_run else " [LIVE]",
        )
        arrow   = "▲" if token_side == "UP" else "▼"
        mode    = "[PAPER]" if is_dry_run else "[LIVE]"
        print(
            f"\n{'═'*62}\n"
            f"  BET PLACED  {arrow} {token_side}  {mode}\n"
            f"  Market  : {market_id}\n"
            f"  Entry   : ${entry_price:.4f}   Size: ${size_usdc:.2f} USDC"
            f"  ({size_shares:.4f} shares)\n"
            f"  P(win)  : {p_true:.1%}   Z-score: {z_score:.3f}\n"
            f"{'═'*62}\n"
        )

    async def monitor_loop(self, state: MarketState) -> None:
        """
        Background loop: checks open position every 500ms for early exit signals.
        Also closes position at expiry if not exited early.
        """
        while True:
            await asyncio.sleep(MONITOR_INTERVAL_S)

            if self._position is None:
                continue

            pos   = self._position
            tau   = state.seconds_to_expiry()
            bid   = pos.current_bid(state)
            age   = pos.age_seconds()

            # ---- Expiry close ----
            if tau <= 0:
                await self._close_at_expiry(pos, state)
                continue

            # Don't exit too early after entry (noise suppression)
            if age < MIN_HOLD_SECONDS:
                continue

            # ---- Take Profit ----
            if bid >= pos.take_profit_threshold():
                logger.info(
                    "TAKE PROFIT: bid=%.4f >= threshold=%.4f (entry=%.4f)",
                    bid, pos.take_profit_threshold(), pos.entry_price,
                )
                await self._close_early(pos, bid, "TAKE_PROFIT", state)
                continue

            # ---- Stop Loss ----
            if bid <= pos.stop_loss_threshold():
                logger.info(
                    "STOP LOSS: bid=%.4f <= threshold=%.4f (entry=%.4f)",
                    bid, pos.stop_loss_threshold(), pos.entry_price,
                )
                await self._close_early(pos, bid, "STOP_LOSS", state)
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
        pnl_usd = (exit_bid - pos.entry_price) * pos.size_shares
        pnl_pct = ((exit_bid / pos.entry_price) - 1.0) * 100
        win     = pnl_usd > 0
        self._finalise(pos, exit_bid, exit_type, pnl_usd, pnl_pct, win)

    async def _close_at_expiry(self, pos: OpenPosition, state: MarketState) -> None:
        """
        At expiry we don't yet know the resolution. We use Polymarket bid as proxy
        (it converges to 0 or 1 at expiry). In dry_run, we read the final
        market bid to determine effective outcome.
        """
        bid = pos.current_bid(state)
        # At or near expiry, bid collapses: >0.90 = very likely WIN, <0.10 = very likely LOSS
        win = bid >= 0.50
        exit_price = 1.0 if win else 0.0
        pnl_usd = (exit_price - pos.entry_price) * pos.size_shares
        pnl_pct = ((exit_price / pos.entry_price) - 1.0) * 100
        self._finalise(pos, exit_price, "EXPIRY", pnl_usd, pnl_pct, win)

    def close_position_with_resolution(
        self,
        resolution: str,   # "WIN" or "LOSS"
        actual_exit_price: float,
    ) -> None:
        """Called externally when Polymarket resolution is known (e.g. from paper_trader resolver)."""
        if self._position is None:
            return
        pos     = self._position
        win     = resolution == "WIN"
        exit_p  = actual_exit_price
        pnl_usd = (exit_p - pos.entry_price) * pos.size_shares
        pnl_pct = ((exit_p / pos.entry_price) - 1.0) * 100
        self._finalise(pos, exit_p, "EXPIRY", pnl_usd, pnl_pct, win)

    def _finalise(
        self,
        pos: OpenPosition,
        exit_price: float,
        exit_type: str,
        pnl_usd: float,
        pnl_pct: float,
        win: bool,
    ) -> None:
        self._position = None  # clear first to avoid re-entry

        # SQLite trade log
        trade_id = log_trade(
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

        # Update the original decision row
        if pos.decision_id:
            update_decision_outcome(
                pos.decision_id,
                f"{'WIN' if win else 'LOSS'} via {exit_type} exit_price={exit_price:.4f}",
            )

        # Build TradeRecord for PnL display
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

        # Permanent CSV log for the dashboard
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

        # Send to Evaluator for post-mortem analysis
        try:
            self.outcome_queue.put_nowait({
                "trade_id": trade_id,
                "pos": pos,
                "exit_price": exit_price,
                "exit_type": exit_type,
                "pnl_usd": pnl_usd,
                "pnl_pct": pnl_pct,
                "win": win,
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
