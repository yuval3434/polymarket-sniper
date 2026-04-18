"""
Developer Agent — Validated Code Injection & Paper Trading Gate
================================================================
Receives strategy proposals from Evaluator and OSINT Researcher.
MANDATORY pipeline:

  Proposal → Backtest → Stage as Candidate → Paper Trade (≥2h / ≥20 trades)
           → Evaluate Gate → PROMOTE to live   OR   REJECT

The Developer agent NEVER promotes directly. Every change must pass the
mandatory paper trading validation period defined in validator.py.

Code snippet injection (when OSINT provides one) is handled safely:
  - Snippets are stored in learning_agent/code_snippets/ for human review
  - They are NOT auto-injected into .py files
  - The developer writes a TODO marker in the snippet file instead
  - A human developer reviews and applies them manually
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone

from core.database import async_log_decision, log_decision, update_decision_outcome
from learning_agent.developer import (
    diff_configs,
    load_live_config,
    promote_candidate,
    reject_candidate,
    stage_candidate,
)
from learning_agent.researcher import BacktestResult, fetch_klines, backtest
from learning_agent.validator import (
    ValidationState,
    ValidationStatus,
    evaluate,
    log_shadow_trade,
    summarise,
)

logger = logging.getLogger(__name__)

_BASE            = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SNIPPET_DIR     = os.path.join(_BASE, "learning_agent", "code_snippets")
_MIN_PROPOSALS_BEFORE_BACKTEST = 2   # accumulate N proposals before running backtest
_PAPER_TRADE_POLL_S = 60             # poll validation state every 60s


class DeveloperAgent:
    def __init__(self, strategy_queue: asyncio.Queue) -> None:
        self.strategy_queue = strategy_queue
        self._pending_proposals: list[dict] = []
        self._active_validation: ValidationState | None = None
        self._experiment_id: str = ""
        os.makedirs(_SNIPPET_DIR, exist_ok=True)

    async def run(self) -> None:
        """Main loop: collect proposals and drive the validation pipeline."""
        while True:
            # Drain the strategy_queue
            try:
                proposal = await asyncio.wait_for(self.strategy_queue.get(), timeout=60)
                await self._ingest_proposal(proposal)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                raise

            # Try to advance the pipeline
            await self._advance_pipeline()

    # ------------------------------------------------------------------
    # Proposal ingestion
    # ------------------------------------------------------------------

    async def _ingest_proposal(self, proposal: dict) -> None:
        source = proposal.get("source", "unknown")
        fixes  = proposal.get("fixes", {})
        logger.info("Developer: received proposal from %s: %s", source, fixes)

        # Store code snippet if present (for human review only)
        snippet = proposal.get("code_snippet")
        if snippet:
            self._save_snippet(snippet, source)

        self._pending_proposals.append(proposal)

    # ------------------------------------------------------------------
    # Pipeline advancement
    # ------------------------------------------------------------------

    async def _advance_pipeline(self) -> None:
        # --- Phase 1: Enough proposals to form a candidate? ---
        if (
            not self._active_validation
            and len(self._pending_proposals) >= _MIN_PROPOSALS_BEFORE_BACKTEST
        ):
            await self._run_backtest_and_stage()

        # --- Phase 2: Active validation running — check gate ---
        if self._active_validation:
            await self._check_validation_gate()

    # ------------------------------------------------------------------
    # Backtest + staging
    # ------------------------------------------------------------------

    async def _run_backtest_and_stage(self) -> None:
        live_cfg = load_live_config()

        # Merge all pending proposals into one candidate config
        import copy
        candidate = copy.deepcopy(live_cfg)
        for p in self._pending_proposals:
            for dotkey, val in p.get("fixes", {}).items():
                keys = dotkey.split(".")
                d = candidate
                for k in keys[:-1]:
                    d = d.setdefault(k, {})
                d[keys[-1]] = val

        self._pending_proposals.clear()

        # Run backtest
        logger.info("Developer: running backtest on merged candidate config")
        candles = await fetch_klines(500)
        if len(candles) < 20:
            logger.error("Developer: insufficient kline data — aborting backtest")
            return

        result: BacktestResult = backtest(candidate, candles)
        logger.info(
            "Developer: backtest result — trades=%d win_rate=%.1f%% pnl=%.4f score=%.4f",
            result.trades_taken, result.win_rate, result.total_pnl, result.score,
        )

        # Gate: must beat minimum viability
        if result.trades_taken < 15 or result.win_rate < 0.52:
            reason = (
                f"Backtest failed: trades={result.trades_taken} "
                f"win_rate={result.win_rate:.1%} (need ≥15 trades, ≥52% WR)"
            )
            logger.info("Developer: %s — proposal rejected at backtest gate", reason)
            await async_log_decision(
                agent="DEVELOPER",
                decision_type="BACKTEST_REJECT",
                details={"reason": reason, "config": candidate},
            )
            return

        # Show diff
        changes = diff_configs(live_cfg, candidate)
        if changes:
            print("\n  Developer Agent — Proposed Changes:\n" + "\n".join(changes))

        # Stage candidate
        exp_id = f"exp_{datetime.now(tz=timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        stage_candidate(
            params=candidate,
            backtest_score=result.score,
            backtest_win_rate=result.win_rate,
            backtest_trades=result.trades_taken,
            experiment_id=exp_id,
        )

        # Start paper trading validation
        self._experiment_id = exp_id
        self._active_validation = ValidationState(experiment_id=exp_id)
        logger.info(
            "Developer: candidate %s staged — starting paper trade validation", exp_id
        )
        await async_log_decision(
            agent="DEVELOPER",
            decision_type="STAGE_CANDIDATE",
            details={
                "experiment_id": exp_id,
                "backtest_win_rate": result.win_rate,
                "backtest_score": result.score,
                "changes": changes,
            },
        )

    # ------------------------------------------------------------------
    # Validation gate
    # ------------------------------------------------------------------

    async def _check_validation_gate(self) -> None:
        if not self._active_validation:
            return

        from learning_agent.reviewer import analyse as reviewer_analyse
        live_report = await asyncio.to_thread(reviewer_analyse)

        status, reason = evaluate(self._active_validation, live_report)
        logger.debug("Developer: validation status — %s", summarise(self._active_validation))

        if status == ValidationStatus.APPROVED:
            vstate = self._active_validation
            logger.info(
                "Developer: PROMOTING candidate %s — %s",
                self._experiment_id, reason,
            )
            print(
                f"\n  ✅  Developer Agent — PROMOTION\n"
                f"  Experiment : {self._experiment_id}\n"
                f"  Reason     : {reason}\n"
            )
            await asyncio.to_thread(
                promote_candidate,
                paper_win_rate=vstate.win_rate,
                paper_trades=vstate.settled_trades,
                experiment_id=self._experiment_id,
            )
            await async_log_decision(
                agent="DEVELOPER",
                decision_type="PROMOTE",
                details={"experiment_id": self._experiment_id, "reason": reason},
            )
            self._active_validation = None
            self._experiment_id = ""

        elif status == ValidationStatus.REJECTED:
            logger.info(
                "Developer: REJECTING candidate %s — %s",
                self._experiment_id, reason,
            )
            print(
                f"\n  ❌  Developer Agent — REJECTION\n"
                f"  Experiment : {self._experiment_id}\n"
                f"  Reason     : {reason}\n"
            )
            await asyncio.to_thread(reject_candidate, reason, self._experiment_id)
            await async_log_decision(
                agent="DEVELOPER",
                decision_type="REJECT",
                details={"experiment_id": self._experiment_id, "reason": reason},
            )
            self._active_validation = None
            self._experiment_id = ""

        # else: still VALIDATING — continue waiting

    # ------------------------------------------------------------------
    # Shadow trade logging (called by orchestrator after each paper trade)
    # ------------------------------------------------------------------

    def record_shadow_trade(
        self,
        market_id: str,
        token_side: str,
        ask_price: float,
        p_true: float,
        z_score: float,
        ev: float,
        would_execute: bool,
    ) -> None:
        if not self._active_validation:
            return
        # Fire-and-forget: CSV write must not block the sniper loop event loop.
        try:
            loop = asyncio.get_running_loop()
            loop.run_in_executor(
                None,
                log_shadow_trade,
                market_id, token_side, ask_price, p_true,
                z_score, ev, would_execute, self._experiment_id,
            )
        except RuntimeError:
            log_shadow_trade(
                market_id=market_id, token_side=token_side,
                ask_price=ask_price, p_true=p_true, z_score=z_score,
                ev=ev, would_execute=would_execute,
                experiment_id=self._experiment_id,
            )

    # ------------------------------------------------------------------
    # Code snippet safe storage
    # ------------------------------------------------------------------

    def _save_snippet(self, code: str, source: str) -> None:
        ts   = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        name = source.replace(":", "_").replace("/", "_")
        path = os.path.join(_SNIPPET_DIR, f"{ts}_{name}.py")
        with open(path, "w") as f:
            f.write(
                f"# === OSINT Code Snippet ===\n"
                f"# Source    : {source}\n"
                f"# Generated : {datetime.now(tz=timezone.utc).isoformat()}\n"
                f"# STATUS    : PENDING HUMAN REVIEW — do not auto-import\n"
                f"# ===========================\n\n"
                + code
            )
        logger.info("Developer: code snippet saved to %s (pending human review)", path)
