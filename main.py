"""
Polymarket 5-Minute BTC Sniper Bot  —  Multi-Agent Edition
===========================================================
Entry point. Starts the fast event loop, bootstraps the Orchestrator,
and runs all agents and streams concurrently.

Usage:
    py main.py                   # paper trading (safe default)
    py main.py --mode paper      # explicit paper mode
    py main.py --mode live       # LIVE trading — real funds at risk
    py main.py --resolve         # score pending paper trades and exit
    py main.py --status          # print session stats from DB and exit
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


import engine.strategy_loader as _scfg
from config import validate_live_credentials
from engine.fee_filter import expected_value, passes_fee_filter
from engine.probability import calculate_probability
from execution.order_manager import OrderManager, _fetch_ask_depth_usdc, _fetch_bid_depth_shares
from market.discovery import discovery_loop
from market.state import MarketState
from monitor.kill_switch import kill_switch_loop
from core.database import async_log_shadow_trade
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
_STATUS_LOG_INTERVAL = 10.0


# ---------------------------------------------------------------------------
# Live-mode startup sequence
# ---------------------------------------------------------------------------

def _live_mode_startup() -> None:
    """Validate credentials then show a 5-second countdown before live trading."""
    validate_live_credentials()   # exits on any missing credential

    banner = "!" * 62
    print(f"\n{banner}")
    print("  ⚠   WARNING — LIVE TRADING MODE ACTIVATED   ⚠")
    print("  REAL FUNDS ARE AT RISK ON EVERY TRADE")
    print(f"  Bet size guardrails: $10 min / $15 max per trade")
    print(f"  Circuit breaker:     $50 drawdown or 20% of bank")
    print(f"  Kill switch:         3 s feed staleness threshold")
    print(f"  Press Ctrl+C NOW to abort.")
    print(f"{banner}\n")

    for i in range(5, 0, -1):
        print(f"  Starting in {i}…", end="\r", flush=True)
        time.sleep(1)
    print(f"  {'─'*56}")
    print(f"  LIVE trading loop starting now.")
    print(f"  {'─'*56}\n")


# ---------------------------------------------------------------------------
# Sniper loop
# ---------------------------------------------------------------------------

async def sniper_loop(
    state: MarketState,
    order_manager: OrderManager,
    orchestrator: Orchestrator,
    dry_run: bool,
) -> None:
    import time as _time
    _last_status = 0.0

    _last_price_reject_key = ("", 0.0)  # (market_id, ask_price) — suppress repeat prints
    _cb_logged = False   # suppress circuit-breaker spam — log once when it trips

    while True:
        await asyncio.sleep(_SNIPE_POLL_INTERVAL)

        if not state.is_ready():
            continue
        if state.kill_switch_active:
            continue
        if orchestrator.risk_manager.halted:
            if not _cb_logged:
                logger.warning("Sniper paused: circuit breaker — %s",
                               orchestrator.risk_manager.halt_reason)
                _cb_logged = True
            continue
        else:
            _cb_logged = False   # reset when breaker clears

        tau = state.seconds_to_expiry()

        window_open  = _scfg.window_open_seconds()
        window_close = _scfg.window_close_seconds()
        stop_buf     = _SNIPE_STOP_BUFFER

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

        if tau < stop_buf:
            continue

        if tau < window_close:
            continue

        if orchestrator.position_manager.has_open_position:
            continue

        P_up, P_down, Z = calculate_probability(state)

        if P_up > P_down:
            token_side, token_id = "UP",   state.up_token_id
            p_true, ask_price    = P_up,   state.up_best_ask
            best_bid, best_ask   = state.up_best_bid, state.up_best_ask
        else:
            token_side, token_id = "DOWN", state.down_token_id
            p_true, ask_price    = P_down, state.down_best_ask
            best_bid, best_ask   = state.down_best_bid, state.down_best_ask

        early_cutoff = _scfg.early_cutoff_seconds()
        if tau > early_cutoff:
            phase      = "EARLY"
            required_z = _scfg.early_z_score()
        else:
            phase      = "STANDARD"
            required_z = _scfg.min_z_score()

        now = _time.monotonic()
        if now - _last_status >= _STATUS_LOG_INTERVAL:
            logger.info(
                "[%s T-%.0fs] BTC=%.2f | side=%s ask=%.4f P=%.3f"
                " Z=%.3f (need≥%.2f) | UP ask=%.4f DOWN ask=%.4f",
                phase, tau, state.btc_price, token_side, ask_price,
                p_true, Z, required_z, state.up_best_ask, state.down_best_ask,
            )
            _last_status = now

        if Z < required_z:
            if Z > 0.05:  # has some signal — worth tracking as missed opportunity
                asyncio.create_task(async_log_shadow_trade(
                    market_id=state.market_id,
                    condition_id=state.condition_id,
                    token_side=token_side,
                    token_id=token_id,
                    theoretical_entry_price=ask_price,
                    rejection_reason="z_too_low",
                    z_score=Z,
                    p_true=p_true,
                    ev=0.0,
                    expiry_ts=state.expiry_ts,
                ))
                logger.debug(
                    "[SHADOW] Skipped %s ask=%.4f Z=%.3f<%.2f (z_too_low) — tracking as missed opportunity",
                    token_side, ask_price, Z, required_z,
                )
            continue

        max_entry = _scfg.max_ask_price()
        if ask_price > max_entry:
            reject_key = (state.market_id, token_side)
            if reject_key != _last_price_reject_key:
                print(
                    f"\n  Trade Rejected: Price ${ask_price:.4f} exceeds"
                    f" max entry ${max_entry:.2f}"
                    f" | T-{tau:.0f}s  side={token_side}  Z={Z:.3f}"
                )
                _last_price_reject_key = reject_key
                asyncio.create_task(async_log_shadow_trade(
                    market_id=state.market_id,
                    condition_id=state.condition_id,
                    token_side=token_side,
                    token_id=token_id,
                    theoretical_entry_price=ask_price,
                    rejection_reason="price_too_high",
                    z_score=Z,
                    p_true=p_true,
                    ev=0.0,
                    expiry_ts=state.expiry_ts,
                ))
                logger.info(
                    "[SHADOW] Skipped %s ask=%.4f>%.2f Z=%.3f (price_too_high) — tracking as missed opportunity",
                    token_side, ask_price, max_entry, Z,
                )
            continue

        # ── OBI depth fetch (async, non-blocking; fails open with depth=0) ────────
        # Fetches cumulative USDC depth within the slippage band on each side
        # so the fee_filter can compute OBI and block toxic order flow.
        bid_depth_usdc = 0.0
        ask_depth_usdc = 0.0
        if _scfg.use_obi_filter():
            try:
                max_price = round(ask_price + 0.03, 4)  # same slippage band as entry guardrail
                ask_depth_usdc, _ = await _fetch_ask_depth_usdc(token_id, max_price)
                bid_min = max(0.01, round(best_bid - 0.03, 4))
                _, bid_depth_usdc = await _fetch_bid_depth_shares(token_id, bid_min)
            except Exception:
                pass  # fail open — missing depth data never blocks a trade

        should_execute, use_maker = passes_fee_filter(
            p_true, ask_price, best_bid, best_ask,
            bid_depth_usdc=bid_depth_usdc,
            ask_depth_usdc=ask_depth_usdc,
        )
        ev = expected_value(p_true, ask_price, is_maker=use_maker)

        log_decision(
            state=state, token_side=token_side, token_id=token_id,
            ask_price=ask_price, p_true=p_true, z_score=Z,
            ev=ev, would_execute=should_execute,
        )

        # on_snipe_signal returns the actual bet_usdc (after live clamp), or None if rejected
        bet_usdc = orchestrator.on_snipe_signal(
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
            asyncio.create_task(async_log_shadow_trade(
                market_id=state.market_id,
                condition_id=state.condition_id,
                token_side=token_side,
                token_id=token_id,
                theoretical_entry_price=ask_price,
                rejection_reason="ev_negative",
                z_score=Z,
                p_true=p_true,
                ev=ev,
                expiry_ts=state.expiry_ts,
            ))
            logger.info(
                "[SHADOW] Skipped %s ask=%.4f EV=%.4f (ev_negative) Z=%.3f — tracking as missed opportunity",
                token_side, ask_price, ev, Z,
            )
            continue

        if bet_usdc is None:
            # Rejected by live guardrail (already logged by risk manager)
            continue

        mode_tag = "[PAPER]" if dry_run else "[LIVE]"
        logger.info(
            "SNIPE %s [%s T-%.1fs]: side=%s ask=%.4f P=%.3f Z=%.3f"
            " EV=%.4f bet=$%.2f maker=%s",
            mode_tag, phase, tau, token_side, ask_price,
            p_true, Z, ev, bet_usdc, use_maker,
        )

        if not dry_run:
            asyncio.create_task(
                _snipe_and_correct_entry(
                    order_manager, orchestrator,
                    token_id=token_id, ask_price=ask_price,
                    use_maker=use_maker, state=state,
                    bet_usdc=bet_usdc, p_true=p_true,
                )
            )


async def _snipe_and_correct_entry(
    order_manager: OrderManager,
    orchestrator: Orchestrator,
    token_id: str,
    ask_price: float,
    use_maker: bool,
    state: MarketState,
    bet_usdc: float,
    p_true: float,
) -> None:
    """Execute the snipe and correct the position's entry price to the actual fill price."""
    result = await order_manager.snipe(
        token_id=token_id, side="BUY",
        ask_price=ask_price, use_maker=use_maker,
        state=state, size_usdc=bet_usdc,
        p_true=p_true,
    )
    if not result:
        return

    # actual_fill_price / actual_fill_shares are only injected for live orders.
    # Paper orders return signal price → no correction needed.
    confirmed_fill   = result.get("actual_fill_price")
    confirmed_shares = result.get("actual_fill_shares")   # on-chain balance delta

    if confirmed_fill:
        actual_price  = float(confirmed_fill)
        actual_shares = float(confirmed_shares) if confirmed_shares is not None else None
    else:
        actual_price  = float(result.get("price") or 0) or float(result.get("averagePrice") or 0)
        actual_shares = None

    if actual_price > 0:
        pos = orchestrator.position_manager._position
        if pos is not None:
            pos.correct_entry_price(actual_price, actual_shares)

    # Tiny fill warning — position is open but notional is far below intended bet
    if result.get("tiny_fill"):
        fill_usdc = result.get("actual_fill_usdc", 0)
        logger.warning(
            "TINY FILL: position opened with $%.2f instead of $%.2f — "
            "book was shallower than pre-check indicated. "
            "Position will be managed normally at corrected entry price.",
            fill_usdc, bet_usdc,
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(dry_run: bool) -> None:
    mode_str = "PAPER (no real funds)" if dry_run else "⚠  LIVE — REAL MONEY"
    banner   = "═" * 62
    print(f"\n{banner}")
    print(f"  Polymarket BTC 5-Min Sniper  ·  Multi-Agent Edition")
    print(f"  Mode: {mode_str}")
    print(f"{banner}\n")

    state         = MarketState()
    order_manager = OrderManager(dry_run=dry_run)
    orchestrator  = Orchestrator(state=state, dry_run=dry_run)
    orchestrator.position_manager.order_manager = order_manager

    await orchestrator.run_all(
        discovery_coro   = discovery_loop(state),
        binance_coro     = binance_stream_loop(state),
        polymarket_coro  = polymarket_stream_loop(state),
        sniper_coro      = sniper_loop(state, order_manager, orchestrator, dry_run),
        kill_switch_coro = kill_switch_loop(
            state, cancel_all_fn=order_manager.cancel_all
        ),
        order_manager    = order_manager,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket BTC Sniper Bot")
    parser.add_argument(
        "--mode",
        choices=["paper", "live"],
        default="paper",
        help="Execution mode: 'paper' (default, safe) or 'live' (real funds)",
    )
    parser.add_argument("--resolve", action="store_true",
                        help="Score pending paper trades and exit")
    parser.add_argument("--status",  action="store_true",
                        help="Print latest session stats from DB and exit")
    parser.add_argument("--reset-cb", action="store_true",
                        help="Reset the circuit breaker and restart trading")
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

    dry_run = (args.mode == "paper")

    if args.reset_cb:
        print("\n  Circuit breaker reset. Starting fresh session.\n")
        # RiskManager is re-initialised on every run — restarting the bot is sufficient.
        # This flag just makes the intent explicit and prints confirmation.

    if not dry_run:
        _live_mode_startup()   # validates credentials + 5s countdown, exits on failure

    try:
        asyncio.run(main(dry_run=dry_run))
    except KeyboardInterrupt:
        logger.info("Interrupted — shutting down")
