"""
Orchestrator — Multi-Agent Coordination Loop
=============================================
Owns all inter-agent queues and wires the full system together.
Replaces the bare asyncio.gather() in main.py with a structured
supervisor that can restart failed agents and exposes shared state.

Agent communication topology:
                                       ┌──────────────┐
  Sniper Loop ──snipe_event──►  Position Manager       │
                                       │  outcome_queue│►  Evaluator
                                       └──────────────┘         │
                                                                 │ strategy_queue
  OSINT Researcher ──────────────────────────────────────────────►
                                                                 ▼
                                                          Developer Agent
                                                          (backtest → stage
                                                           → paper trade
                                                           → promote/reject)

Shared state:
  - MarketState  : live prices + order book (read by all agents)
  - RiskManager  : circuit breaker + Kelly sizing
  - PnLTracker   : live display
"""
from __future__ import annotations

import asyncio
import logging

from agents.developer        import DeveloperAgent
from agents.evaluator        import Evaluator
from agents.osint_researcher import OSINTResearcher
from agents.position_manager import PositionManager
from core.database           import init_db, log_tick
from core.pnl_tracker        import PnLTracker
from core.risk_manager       import RiskManager
from learning_agent.agent    import LearningAgent
from market.state            import MarketState

logger = logging.getLogger(__name__)

_TICK_LOG_INTERVAL = 5   # log a tick to SQLite every N seconds


class Orchestrator:
    def __init__(self, state: MarketState, dry_run: bool = True) -> None:
        self.state    = state
        self.dry_run  = dry_run

        # Inter-agent queues
        self.outcome_queue  = asyncio.Queue(maxsize=100)
        self.strategy_queue = asyncio.Queue(maxsize=50)

        # Shared services
        self.pnl_tracker   = PnLTracker(strategy_version=1)
        self.risk_manager  = RiskManager(starting_bank=100.0)

        # Agents
        self.position_manager = PositionManager(
            pnl_tracker=self.pnl_tracker,
            risk_manager=self.risk_manager,
            outcome_queue=self.outcome_queue,
        )
        self.evaluator        = Evaluator(self.outcome_queue, self.strategy_queue)
        self.osint_researcher = OSINTResearcher(self.strategy_queue)
        self.developer        = DeveloperAgent(self.strategy_queue)
        self.learning_agent   = LearningAgent()

        # Init SQLite
        init_db()
        logger.info("Orchestrator initialised (dry_run=%s)", dry_run)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def run_all(
        self,
        discovery_coro,
        binance_coro,
        polymarket_coro,
        sniper_coro,
        kill_switch_coro,
    ) -> None:
        """
        Launch all coroutines under a supervised gather.
        If any agent crashes it is logged and restarted (except kill_switch).
        """
        await asyncio.gather(
            discovery_coro,
            binance_coro,
            polymarket_coro,
            sniper_coro,
            kill_switch_coro,
            self._run_agent("PositionMonitor",  self.position_manager.monitor_loop(self.state)),
            self._run_agent("Evaluator",         self.evaluator.run()),
            self._run_agent("OSINTResearcher",   self.osint_researcher.run()),
            self._run_agent("Developer",         self.developer.run()),
            self._run_agent("LearningAgent",     self.learning_agent.run()),
            self._tick_logger(),
            return_exceptions=False,
        )

    async def _run_agent(self, name: str, coro) -> None:
        """Wrap a coroutine with auto-restart on unexpected failure."""
        while True:
            try:
                await coro
                break  # normal exit
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Agent %s crashed: %s — restarting in 5s", name, exc, exc_info=True)
                await asyncio.sleep(5)

    # ------------------------------------------------------------------
    # Tick logger (samples state → SQLite every 5s)
    # ------------------------------------------------------------------

    async def _tick_logger(self) -> None:
        import time
        last = time.monotonic()
        while True:
            await asyncio.sleep(1)
            if time.monotonic() - last >= _TICK_LOG_INTERVAL:
                last = time.monotonic()
                if self.state.btc_price > 0:
                    log_tick(
                        market_id=self.state.market_id,
                        btc_price=self.state.btc_price,
                        up_ask=self.state.up_best_ask,
                        down_ask=self.state.down_best_ask,
                        up_bid=self.state.up_best_bid,
                        down_bid=self.state.down_best_bid,
                    )

    # ------------------------------------------------------------------
    # Called by sniper_loop when a snipe signal fires
    # ------------------------------------------------------------------

    def on_snipe_signal(
        self,
        token_side: str,
        token_id: str,
        ask_price: float,
        p_true: float,
        z_score: float,
        ev: float,
        would_execute: bool,
    ) -> None:
        """
        Called for every snipe evaluation (whether executed or not).
        Routes to PositionManager and DeveloperAgent shadow trader.
        """
        if not would_execute:
            return

        if self.risk_manager.halted:
            logger.warning("Snipe blocked: circuit breaker is active")
            return

        # Dynamic bet sizing
        bet_usdc = self.risk_manager.kelly_bet_usdc(p_true)

        # Open position in PositionManager
        if not self.position_manager.has_open_position:
            self.position_manager.open_position(
                market_id=self.state.market_id,
                condition_id=self.state.condition_id,
                token_side=token_side,
                token_id=token_id,
                entry_price=ask_price,
                p_true=p_true,
                z_score=z_score,
                size_usdc=bet_usdc,
                is_dry_run=self.dry_run,
            )

        # Shadow-trade the candidate config (if validation is active)
        self.developer.record_shadow_trade(
            market_id=self.state.market_id,
            token_side=token_side,
            ask_price=ask_price,
            p_true=p_true,
            z_score=z_score,
            ev=ev,
            would_execute=would_execute,
        )

    def status(self) -> str:
        return (
            f"[Orchestrator] {self.risk_manager.status_line()}  |  "
            f"Position: {'OPEN' if self.position_manager.has_open_position else 'none'}"
        )
