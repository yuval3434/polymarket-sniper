"""
Learning Agent — Autonomous Improvement Orchestrator
=====================================================
Coordinates the Reviewer → Researcher → Developer → Validator pipeline
in a continuous background loop.

Can run in two modes:
  1. Embedded  : imported by agents/orchestrator.py, runs as an asyncio task
  2. Standalone: `py -m learning_agent.agent` for offline analysis/testing

Cycle schedule:
  Every REVIEW_INTERVAL_S    → Reviewer  : analyse paper_trades.csv
  Every RESEARCH_INTERVAL_S  → Researcher: backtest new candidates
  Every VALIDATION_POLL_S    → Validator : check paper-trade gate
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

from learning_agent.developer  import (
    diff_configs,
    load_live_config,
    promote_candidate,
    reject_candidate,
    stage_candidate,
    load_candidate_config,
)
from learning_agent.researcher import research, BacktestResult
from learning_agent.reviewer   import analyse, AnalysisReport
from learning_agent.validator  import (
    ValidationState,
    ValidationStatus,
    evaluate,
    summarise,
)

logger = logging.getLogger(__name__)

REVIEW_INTERVAL_S      = 3600        # run Reviewer every 1 hour
RESEARCH_INTERVAL_S    = 3600 * 2   # run Researcher every 2 hours
VALIDATION_POLL_S      = 60          # check validation gate every 60 s
MIN_TRADES_FOR_REVIEW  = 5           # don't bother if fewer resolved trades exist


class LearningAgent:
    """
    Self-contained async agent. Call `await agent.run()` to start.
    """

    def __init__(self) -> None:
        self._last_review_ts:   float = 0.0
        self._last_research_ts: float = 0.0
        self._active_validation: ValidationState | None = None
        self._experiment_id: str = ""

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        logger.info("LearningAgent started")
        while True:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("LearningAgent tick error: %s", exc, exc_info=True)
            await asyncio.sleep(VALIDATION_POLL_S)

    async def _tick(self) -> None:
        now = time.monotonic()

        # ── 1. Review ────────────────────────────────────────────────
        if now - self._last_review_ts >= REVIEW_INTERVAL_S:
            self._last_review_ts = now
            report = await asyncio.get_event_loop().run_in_executor(None, analyse)
            self._print_review(report)

        # ── 2. Research (only if no validation currently active) ─────
        if (
            now - self._last_research_ts >= RESEARCH_INTERVAL_S
            and self._active_validation is None
        ):
            self._last_research_ts = now
            await self._research_cycle()

        # ── 3. Validation gate check ─────────────────────────────────
        if self._active_validation is not None:
            await self._check_gate()

    # ------------------------------------------------------------------
    # Research cycle
    # ------------------------------------------------------------------

    async def _research_cycle(self) -> None:
        report = await asyncio.get_event_loop().run_in_executor(None, analyse)
        if report.executed_trades < MIN_TRADES_FOR_REVIEW:
            logger.info(
                "LearningAgent: only %d resolved trades — skipping research cycle",
                report.executed_trades,
            )
            return

        live_cfg = await asyncio.to_thread(load_live_config)
        logger.info("LearningAgent: running research cycle…")

        viable: list[BacktestResult] = await research(live_cfg, report, n_random=8)
        if not viable:
            logger.info("LearningAgent: no viable candidates from research cycle")
            return

        best = viable[0]

        # Show what's changing vs live
        changes = diff_configs(live_cfg, best.params)
        if changes:
            print("\n  LearningAgent — Proposed parameter changes:")
            for c in changes:
                print(c)

        import uuid
        exp_id = (
            f"la_{datetime.now(tz=timezone.utc).strftime('%Y%m%d_%H%M%S')}"
            f"_{uuid.uuid4().hex[:6]}"
        )

        await asyncio.to_thread(
            stage_candidate,
            params=best.params,
            backtest_score=best.score,
            backtest_win_rate=best.win_rate,
            backtest_trades=best.trades_taken,
            experiment_id=exp_id,
        )

        self._experiment_id     = exp_id
        self._active_validation = ValidationState(experiment_id=exp_id)

        logger.info(
            "LearningAgent: staged candidate %s — paper trading validation started",
            exp_id,
        )
        print(
            f"\n  {'═'*58}\n"
            f"  LearningAgent: New candidate staged\n"
            f"  Experiment  : {exp_id}\n"
            f"  Backtest WR : {best.win_rate:.1%}  trades={best.trades_taken}"
            f"  score={best.score:.4f}\n"
            f"  {'═'*58}\n"
        )

    # ------------------------------------------------------------------
    # Validation gate
    # ------------------------------------------------------------------

    async def _check_gate(self) -> None:
        if self._active_validation is None:
            return

        report = await asyncio.get_event_loop().run_in_executor(None, analyse)
        status, reason = evaluate(self._active_validation, report)

        logger.debug("LearningAgent gate: %s", summarise(self._active_validation))

        if status == ValidationStatus.APPROVED:
            vstate = self._active_validation
            await asyncio.to_thread(
                promote_candidate,
                paper_win_rate=vstate.win_rate,
                paper_trades=vstate.settled_trades,
                experiment_id=self._experiment_id,
            )
            print(
                f"\n  ✅  LearningAgent: PROMOTED {self._experiment_id}\n"
                f"  {reason}\n"
            )
            self._active_validation = None
            self._experiment_id     = ""

        elif status == ValidationStatus.REJECTED:
            await asyncio.to_thread(reject_candidate, reason, self._experiment_id)
            print(
                f"\n  ❌  LearningAgent: REJECTED {self._experiment_id}\n"
                f"  {reason}\n"
            )
            self._active_validation = None
            self._experiment_id     = ""

    # ------------------------------------------------------------------
    # Pretty-print helpers
    # ------------------------------------------------------------------

    def _print_review(self, report: AnalysisReport) -> None:
        div = "─" * 58
        print(
            f"\n  {div}\n"
            f"  LearningAgent — Review @ {datetime.now(tz=timezone.utc).strftime('%H:%M UTC')}\n"
            f"  {div}\n"
            f"  {report.summary()}\n"
            f"  {div}\n"
        )

    # ------------------------------------------------------------------
    # External hook — called by agents/developer.py to update shadow trades
    # ------------------------------------------------------------------

    def get_active_experiment_id(self) -> str:
        return self._experiment_id

    def get_active_validation(self) -> ValidationState | None:
        return self._active_validation


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

async def _standalone() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )
    agent = LearningAgent()

    # One-shot: run a single review + research cycle then exit
    report = analyse()
    agent._print_review(report)

    if report.executed_trades >= MIN_TRADES_FOR_REVIEW:
        await agent._research_cycle()
        print("\nRun `py main.py` to start live paper trading of the staged candidate.")
    else:
        print(
            f"\nOnly {report.executed_trades} resolved trades available.\n"
            f"Run the bot in paper mode for a few hours first, then re-run this script."
        )


if __name__ == "__main__":
    asyncio.run(_standalone())
