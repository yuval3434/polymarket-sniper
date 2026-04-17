"""
Polymarket 5-Minute BTC Sniper Bot  —  Multi-Agent Edition
===========================================================
Entry point. Starts the fast event loop, bootstraps the Orchestrator,
and runs all agents and streams concurrently.

Usage:
    py main.py                  # paper trade (DRY_RUN=true in .env)
    py main.py --resolve        # score pending paper trades in CSV and exit
    py main.py --status         # print session stats and exit
"""

import argparse
import asyncio
import logging
import sys
import time

import sys as _sys


def _install_fast_loop() -> None:
    if _sys.platform == "win32":
        try:
            import winloop; winloop.install(); return
        except ImportError:
            pass
    else:
        try:
            import uvloop; uvloop.install(); return
        except ImportError:
            pass


from config import DRY_RUN
import engine.strategy_loader as _scfg
from engine.fee_filter import expected_value, passes_fee_filter
from engine.probability import calculate_probability
from execution.order_manager import OrderManager
from market.discovery import discovery_loop
from market.state import MarketState
from monitor.kill_switch import kill_switch_loop
from simulation.paper_trader import log_decision, resolve_pending_trades
from streams.binance_stream import binance_stream_loop
from streams.polymarket_stream import polymarket_stream_loop
from agents.orchestrator import Orchestrator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

_SNIPE_POLL_INTERVAL = 0.1
_SNIPE_STOP_BUFFER   = 2.0
_STATUS_LOG_INTERVAL = 10.0   # heartbeat log interval in seconds


# ---------------------------------------------------------------------------
# Sniper loop
# ---------------------------------------------------------------------------

async def sniper_loop(
    state: MarketState,
    order_manager: OrderManager,
    orchestrator: Orchestrator,
) -> None:
    import time as _time
    _last_status = 0.0

    while True:
        await asyncio.sleep(_SNIPE_POLL_INTERVAL)

        if not state.is_ready():
            continue
        if state.kill_switch_active:
            continue
        if orchestrator.risk_manager.halted:
            logger.warning("Sniper paused: circuit breaker — %s",
                           orchestrator.risk_manager.halt_reason)
            continue

        tau = state.seconds_to_expiry()

        # ── Regime boundaries (all read live from strategy_config.json) ─
        window_open  = _scfg.window_open_seconds()    # start of entry window  (~T-290s)
        window_close = _scfg.window_close_seconds()   # hard entry cutoff      (T-90s)
        stop_buf     = _SNIPE_STOP_BUFFER             # stop all activity      (T-2s)

        # ── Before market entry window: periodic heartbeat ────────────
        if tau > window_open:
            now = _time.monotonic()
            if now - _last_status >= _STATUS_LOG_INTERVAL:
                logger.info(
                    "Waiting for market window | T-%.0fs | BTC=%.2f"
                    " | UP ask=%.4f DOWN ask=%.4f",
                    tau, state.btc_price, state.up_best_ask, state.down_best_ask,
                )
                _last_status = now
            continue

        # ── Past all activity ─────────────────────────────────────────
        if tau < stop_buf:
            continue

        # ── Hard time gate: last 90s = monitoring only, no new entries ─
        if tau < window_close:
            continue

        # ── Already entered this market window ────────────────────────
        if state.snipe_fired or orchestrator.position_manager.has_open_position:
            continue

        # ── Pick best side ────────────────────────────────────────────
        P_up, P_down, Z = calculate_probability(state)

        if P_up > P_down:
            token_side, token_id = "UP",   state.up_token_id
            p_true, ask_price    = P_up,   state.up_best_ask
            best_bid, best_ask   = state.up_best_bid, state.up_best_ask
        else:
            token_side, token_id = "DOWN", state.down_token_id
            p_true, ask_price    = P_down, state.down_best_ask
            best_bid, best_ask   = state.down_best_bid, state.down_best_ask

        # ── Determine phase and required Z threshold ──────────────────
        #   Minutes 1-2  (tau > early_cutoff): EARLY  — lower bar
        #   Minute 3     (tau ≤ early_cutoff): STANDARD
        early_cutoff = _scfg.early_cutoff_seconds()
        if tau > early_cutoff:
            phase      = "EARLY"
            required_z = _scfg.early_z_score()
        else:
            phase      = "STANDARD"
            required_z = _scfg.min_z_score()

        # ── Periodic eval status (every 10s inside window) ────────────
        now = _time.monotonic()
        if now - _last_status >= _STATUS_LOG_INTERVAL:
            logger.info(
                "[%s T-%.0fs] BTC=%.2f | side=%s ask=%.4f P=%.3f"
                " Z=%.3f (need≥%.2f) | UP ask=%.4f DOWN ask=%.4f",
                phase, tau, state.btc_price, token_side, ask_price,
                p_true, Z, required_z, state.up_best_ask, state.down_best_ask,
            )
            _last_status = now

        # ── Gate 1: Z-score (confidence in direction) ─────────────────
        if Z < required_z:
            # Too close to 50/50 — silent, re-evaluated every 100ms
            continue

        # ── Gate 2: Max entry price ($0.75 hard cap) ──────────────────
        max_entry = _scfg.max_ask_price()
        if ask_price > max_entry:
            print(
                f"\n  Trade Rejected: Price ${ask_price:.4f} exceeds"
                f" max entry ${max_entry:.2f}"
                f" | T-{tau:.0f}s  side={token_side}  Z={Z:.3f}"
            )
            continue

        # ── Gate 3: Expected Value / fee filter ───────────────────────
        should_execute, use_maker = passes_fee_filter(p_true, ask_price, best_bid, best_ask)
        ev = expected_value(p_true, ask_price, is_maker=use_maker)

        # ── Always log to paper trades CSV ───────────────────────────
        log_decision(
            state=state, token_side=token_side, token_id=token_id,
            ask_price=ask_price, p_true=p_true, z_score=Z,
            ev=ev, would_execute=should_execute,
        )

        # ── Notify orchestrator (opens position + shadow trade) ──────
        orchestrator.on_snipe_signal(
            token_side=token_side, token_id=token_id,
            ask_price=ask_price, p_true=p_true,
            z_score=Z, ev=ev, would_execute=should_execute,
        )

        if not should_execute:
            print(
                f"\n  Trade Rejected: EV={ev:.4f} below safety buffer"
                f" ({_scfg.safety_buffer():.2f})"
                f" | T-{tau:.0f}s  side={token_side}"
                f"  ask=${ask_price:.4f}  P={p_true:.3f}"
            )
            continue

        # ── All gates passed → execute ────────────────────────────────
        state.snipe_fired = True

        logger.info(
            "SNIPE [%s] T-%.1fs: side=%s ask=%.4f P=%.3f Z=%.3f EV=%.4f maker=%s%s",
            phase, tau, token_side, ask_price, p_true, Z, ev, use_maker,
            " [DRY RUN]" if DRY_RUN else " [LIVE]",
        )

        if not DRY_RUN:
            asyncio.create_task(
                order_manager.snipe(
                    token_id=token_id, side="BUY",
                    ask_price=ask_price, use_maker=use_maker, state=state,
                )
            )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    mode = "DRY RUN (paper trading)" if DRY_RUN else "⚠  LIVE TRADING — REAL MONEY"
    banner = "═" * 62
    print(f"\n{banner}")
    print(f"  Polymarket BTC 5-Min Sniper  ·  Multi-Agent Edition")
    print(f"  Mode: {mode}")
    print(f"{banner}\n")

    state         = MarketState()
    order_manager = OrderManager()
    orchestrator  = Orchestrator(state=state, dry_run=DRY_RUN)

    await orchestrator.run_all(
        discovery_coro   = discovery_loop(state),
        binance_coro     = binance_stream_loop(state),
        polymarket_coro  = polymarket_stream_loop(state),
        sniper_coro      = sniper_loop(state, order_manager, orchestrator),
        kill_switch_coro = kill_switch_loop(
            state, cancel_all_fn=order_manager.cancel_all
        ),
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resolve", action="store_true",
                        help="Score pending paper trades and exit")
    parser.add_argument("--status",  action="store_true",
                        help="Print latest session stats from DB and exit")
    args = parser.parse_args()

    _install_fast_loop()

    if args.resolve:
        asyncio.run(resolve_pending_trades())
        sys.exit(0)

    if args.status:
        from core.database import fetch_recent_trades
        trades = fetch_recent_trades(50)
        wins   = sum(1 for t in trades if t.get("win"))
        total  = len(trades)
        pnl    = sum(t.get("pnl_usd", 0) for t in trades)
        print(f"Last {total} trades: {wins}W/{total-wins}L  WR={wins/total:.1%}  PnL=${pnl:+.4f}")
        sys.exit(0)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted — shutting down")
