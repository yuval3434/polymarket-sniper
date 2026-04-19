"""
Risk Manager — Circuit Breaker & Dynamic Bet Sizing
=====================================================
Controls how much capital to risk per trade (Kelly-fractional sizing)
and halts trading if session drawdown exceeds the configured limit.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

# Defaults (overridden by strategy_config.json)
_MAX_DRAWDOWN_HALT_USD  = float(os.environ.get("MAX_DRAWDOWN_HALT_USD", "50.0"))
_MAX_DRAWDOWN_HALT_PCT  = float(os.environ.get("MAX_DRAWDOWN_HALT_PCT", "30.0"))  # % of starting bank (was 20%, too tight for $10 bets)
_KELLY_FRACTION         = float(os.environ.get("KELLY_FRACTION", "0.25"))         # quarter-Kelly
_MIN_BET_USDC           = float(os.environ.get("MIN_BET_USDC", "1.0"))
_MAX_BET_USDC           = float(os.environ.get("MAX_BET_USDC", "25.0"))
_DEFAULT_BET_USDC       = float(os.environ.get("MAX_POSITION_USDC", "10.0"))

# Hard-coded live-mode guardrails — never changed by config or Kelly
LIVE_MIN_BET_USDC: float = 10.0
LIVE_MAX_BET_USDC: float = 15.0


class RiskManager:
    def __init__(
        self,
        starting_bank: float = 100.0,
        max_drawdown_usd: float = _MAX_DRAWDOWN_HALT_USD,
        max_drawdown_pct: float = _MAX_DRAWDOWN_HALT_PCT,
        kelly_fraction: float = _KELLY_FRACTION,
    ) -> None:
        self.starting_bank    = starting_bank
        self.current_bank     = starting_bank
        self.max_drawdown_usd = max_drawdown_usd
        self.max_drawdown_pct = max_drawdown_pct
        self.kelly_fraction   = kelly_fraction
        self.peak_bank        = starting_bank
        self.halted           = False
        self.halt_reason      = ""
        self._total_trades    = 0
        self._wins            = 0

    # ------------------------------------------------------------------
    # Bank management
    # ------------------------------------------------------------------

    def record_trade_result(self, pnl_usd: float, win: bool) -> None:
        self.current_bank += pnl_usd
        self.peak_bank = max(self.peak_bank, self.current_bank)
        self._total_trades += 1
        if win:
            self._wins += 1
        self._check_circuit_breaker()

    def _check_circuit_breaker(self) -> None:
        drawdown_usd = self.peak_bank - self.current_bank
        drawdown_pct = (drawdown_usd / self.starting_bank) * 100

        if drawdown_usd >= self.max_drawdown_usd:
            self.halted = True
            self.halt_reason = (
                f"Drawdown ${drawdown_usd:.2f} exceeded limit ${self.max_drawdown_usd:.2f}"
            )
            logger.critical("CIRCUIT BREAKER TRIPPED: %s", self.halt_reason)

        elif drawdown_pct >= self.max_drawdown_pct:
            self.halted = True
            self.halt_reason = (
                f"Drawdown {drawdown_pct:.1f}% exceeded limit {self.max_drawdown_pct:.1f}%"
            )
            logger.critical("CIRCUIT BREAKER TRIPPED: %s", self.halt_reason)

    def reset_circuit_breaker(self) -> None:
        """Manual reset only — not called automatically."""
        self.halted = False
        self.halt_reason = ""
        logger.warning("Circuit breaker reset manually")

    def force_halt(self, reason: str) -> None:
        """Immediate halt regardless of drawdown — used for critical execution failures."""
        self.halted = True
        self.halt_reason = reason
        logger.critical("FORCED HALT: %s", reason)

    # ------------------------------------------------------------------
    # Kelly-fractional bet sizing
    # ------------------------------------------------------------------

    def kelly_bet_usdc(self, p_win: float, odds: float = 1.0) -> float:
        """
        Quarter-Kelly bet size in USDC.

        Kelly formula: f* = (p*b - (1-p)) / b
          where b = net odds (payout per unit risked, typically 1.0 here
          because a $1 share pays $1 at expiry on Polymarket).

        We use a reduced fraction (quarter-Kelly by default) for safety.
        Returns the raw Kelly bet — call apply_live_clamp() for live guardrails.
        """
        if p_win <= 0 or p_win >= 1:
            return _DEFAULT_BET_USDC

        b = odds
        f_star = (p_win * b - (1 - p_win)) / b
        f_star = max(0.0, f_star)  # never bet negative
        bet = self.kelly_fraction * f_star * self.current_bank
        bet = max(_MIN_BET_USDC, min(_MAX_BET_USDC, bet))
        return round(bet, 2)

    def apply_live_clamp(self, kelly_bet: float) -> float | None:
        """
        Enforce live-mode hard guardrails: $10 min / $15 max.

        Returns the clamped bet size, or None if the trade must be rejected
        (Kelly suggested ≤ 0, meaning no statistical edge was found).

        Logs a warning whenever the bet is adjusted.
        """
        if kelly_bet <= 0:
            logger.warning(
                "Live guardrail: Kelly bet=%.2f ≤ 0 — no edge detected, trade REJECTED",
                kelly_bet,
            )
            return None

        clamped = kelly_bet

        if kelly_bet < LIVE_MIN_BET_USDC:
            clamped = LIVE_MIN_BET_USDC
            logger.warning(
                "Live guardrail: Kelly bet $%.2f < minimum $%.2f — "
                "flooring to $%.2f  [GUARDRAIL APPLIED]",
                kelly_bet, LIVE_MIN_BET_USDC, clamped,
            )
        elif kelly_bet > LIVE_MAX_BET_USDC:
            clamped = LIVE_MAX_BET_USDC
            logger.warning(
                "Live guardrail: Kelly bet $%.2f > maximum $%.2f — "
                "capping to $%.2f  [GUARDRAIL APPLIED]",
                kelly_bet, LIVE_MAX_BET_USDC, clamped,
            )
        else:
            logger.debug("Live guardrail: Kelly bet $%.2f is within [$%.2f, $%.2f] ✓",
                         kelly_bet, LIVE_MIN_BET_USDC, LIVE_MAX_BET_USDC)

        return round(clamped, 2)

    # ------------------------------------------------------------------
    # Rolling win rate
    # ------------------------------------------------------------------

    @property
    def rolling_win_rate(self) -> float:
        return self._wins / self._total_trades if self._total_trades > 0 else 0.5

    def status_line(self) -> str:
        dd = self.peak_bank - self.current_bank
        return (
            f"Bank: ${self.current_bank:.2f}  "
            f"Peak: ${self.peak_bank:.2f}  "
            f"DD: ${dd:.2f}  "
            f"WR: {self.rolling_win_rate:.1%}  "
            f"{'⛔ HALTED' if self.halted else '✓ active'}"
        )
