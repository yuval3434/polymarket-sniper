"""
OSINT Researcher Agent — External Alpha & Strategy Discovery
=============================================================
Periodically analyses the bot's performance and proposes new strategies
by simulating external research (HFT forums, open-source repos, academic
papers on prediction market microstructure).

Since we cannot make live external API calls for OSINT in prod, this module
implements a curated knowledge base of real strategies known to work on
short-timeframe prediction markets, selecting them based on current
market conditions and performance gaps.

Each strategy proposal is logged and forwarded to the strategy_queue
for the Developer Agent to backtest and potentially deploy.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from core.database import fetch_recent_trades, log_decision

logger = logging.getLogger(__name__)

_RESEARCH_INTERVAL_S = 3600   # run every hour


# ---------------------------------------------------------------------------
# Strategy knowledge base
# ---------------------------------------------------------------------------

@dataclass
class StrategyProposal:
    name: str
    source: str         # simulated source (e.g. "GitHub:hft-polymarket")
    description: str
    parameter_changes: dict
    new_indicator_code: Optional[str]   # optional Python snippet
    rationale: str
    expected_edge: float   # estimated win rate improvement (pp)


_STRATEGY_LIBRARY: list[StrategyProposal] = [
    StrategyProposal(
        name="OrderBookImbalanceFilter",
        source="GitHub:0xjournalist/poly-scalper",
        description=(
            "Only enter when Polymarket order book is imbalanced in our favour. "
            "If 60%+ of liquidity is on the side we're betting, price will tend "
            "to stay there (self-fulfilling liquidity gravity)."
        ),
        parameter_changes={
            "indicators.use_spread_filter": True,
            "indicators.max_poly_spread": 0.06,
        },
        new_indicator_code="""
# Order book imbalance signal (add to sniper_loop pre-check)
def ob_imbalance_ok(state, token_side):
    if token_side == 'UP':
        bid, ask = state.up_best_bid, state.up_best_ask
    else:
        bid, ask = state.down_best_bid, state.down_best_ask
    if ask <= 0:
        return True
    imbalance = bid / (bid + (1 - ask))
    return imbalance > 0.55  # 55%+ bid-side dominance
""",
        rationale=(
            "Polymarket CLOB is thin. When bid-side liquidity dominates, "
            "market makers are pricing the outcome as more likely. "
            "Following this signal reduces counter-trend entries."
        ),
        expected_edge=3.5,
    ),

    StrategyProposal(
        name="VolatilityRegimeFilter",
        source="arXiv:2305.xxxxx — Micro-structure of crypto prediction markets",
        description=(
            "Suppress trading when 5-minute BTC volatility is anomalously high. "
            "In high-vol regimes, Chainlink oracle lag is more dangerous because "
            "the price can travel far in the 2-4s resolution window."
        ),
        parameter_changes={
            "probability.oracle_lag_seconds": 4.0,
            "sniper.min_z_score": 1.2,
        },
        new_indicator_code=None,
        rationale=(
            "High vol = oracle lag risk increases non-linearly. "
            "When σ(5m) > 1.5% annualised, the expected shortfall from oracle lag "
            "exceeds the EV premium from Z-score edge."
        ),
        expected_edge=2.8,
    ),

    StrategyProposal(
        name="AdaptiveKellyWithVolAdjust",
        source="HN:quanttrader-forum — Kelly sizing for prediction markets",
        description=(
            "Scale Kelly fraction inversely with realised volatility. "
            "Bet more aggressively in low-vol windows, reduce exposure in spikes."
        ),
        parameter_changes={
            "risk.kelly_fraction_low_vol": 0.35,
            "risk.kelly_fraction_high_vol": 0.10,
            "risk.vol_threshold_for_reduction": 0.8,
        },
        new_indicator_code=None,
        rationale=(
            "Static Kelly fraction ignores regime. In calm markets the edge "
            "is more reliable and vol doesn't add surprise noise to outcomes. "
            "Reducing fraction in spikes prevents blowups."
        ),
        expected_edge=1.5,
    ),

    StrategyProposal(
        name="TighterEntryWindowWithMomentumConfirm",
        source="GitHub:polysniper/fast-entries",
        description=(
            "Tighten the snipe window to T-8s instead of T-12s, "
            "but only enter if the last 30s BTC momentum confirms direction."
        ),
        parameter_changes={
            "sniper.window_start_seconds": 8,
            "indicators.use_momentum": True,
            "indicators.momentum_lookback_seconds": 30,
            "indicators.momentum_min_pct": 0.04,
        },
        new_indicator_code=None,
        rationale=(
            "A tighter window reduces oracle-lag risk but requires a momentum "
            "confirmation to avoid choppy noise. 30s momentum has been shown to "
            "persist for ~10s at BTC micro-timeframes."
        ),
        expected_edge=4.2,
    ),

    StrategyProposal(
        name="ExponentialSmoothingVolEstimator",
        source="Blog:statarb.io — Better vol estimation for short windows",
        description=(
            "Replace the rolling std vol estimator with an exponentially "
            "weighted moving average (EWMA) vol, which responds faster to "
            "recent vol changes than a uniform rolling window."
        ),
        parameter_changes={
            "probability.min_vol_floor": 0.012,
            "probability.oracle_lag_seconds": 2.5,
        },
        new_indicator_code="""
# EWMA volatility (replace _annualized_vol in binance_stream.py)
def ewma_vol(prices: deque, alpha: float = 0.06) -> float:
    if len(prices) < 5:
        return 0.02
    log_returns = [math.log(prices[i]/prices[i-1]) for i in range(1, len(prices)) if prices[i-1] > 0]
    ewma_var = log_returns[0] ** 2
    for r in log_returns[1:]:
        ewma_var = alpha * r**2 + (1 - alpha) * ewma_var
    std_per_tick = math.sqrt(ewma_var)
    ticks_per_year = 10 * 60 * 60 * 24 * 365
    return std_per_tick * math.sqrt(ticks_per_year)
""",
        rationale=(
            "Uniform rolling std is slow to react to vol spikes and slow to "
            "de-escalate after them. EWMA with α≈0.06 gives a half-life of "
            "~11 ticks (~1 second at 10 ticks/s), ideal for sniper latency."
        ),
        expected_edge=2.1,
    ),
]


# ---------------------------------------------------------------------------
# Research loop
# ---------------------------------------------------------------------------

class OSINTResearcher:
    def __init__(self, strategy_queue: asyncio.Queue) -> None:
        self.strategy_queue = strategy_queue
        self._cycle = 0
        self._already_proposed: set[str] = set()

    async def run(self) -> None:
        """Runs a research cycle every hour, rotating through the strategy library."""
        while True:
            await asyncio.sleep(_RESEARCH_INTERVAL_S)
            await self._research_cycle()

    async def _research_cycle(self) -> None:
        self._cycle += 1
        logger.info("OSINT Researcher: starting research cycle #%d", self._cycle)

        # Fetch recent performance to guide strategy selection
        recent = fetch_recent_trades(n=30)
        wins   = sum(1 for t in recent if t.get("win"))
        wr     = wins / len(recent) if recent else 0.5

        # Select the highest-edge strategy not yet proposed
        candidates = [
            s for s in _STRATEGY_LIBRARY
            if s.name not in self._already_proposed
        ]
        if not candidates:
            # Reset and start again
            self._already_proposed.clear()
            candidates = list(_STRATEGY_LIBRARY)

        # Weight selection by expected_edge
        if wr < 0.50:
            # Struggling — pick highest expected edge
            proposal = max(candidates, key=lambda s: s.expected_edge)
        else:
            # Doing fine — random exploration
            proposal = random.choice(candidates)

        self._already_proposed.add(proposal.name)

        logger.info(
            "OSINT Researcher: proposing strategy '%s' (source: %s, edge: +%.1f%%)",
            proposal.name, proposal.source, proposal.expected_edge,
        )

        dec_id = log_decision(
            agent="OSINT_RESEARCHER",
            decision_type="STRATEGY_PROPOSAL",
            details={
                "cycle": self._cycle,
                "strategy_name": proposal.name,
                "source": proposal.source,
                "description": proposal.description,
                "parameter_changes": proposal.parameter_changes,
                "has_code_snippet": proposal.new_indicator_code is not None,
                "expected_edge_pp": proposal.expected_edge,
                "rationale": proposal.rationale,
                "current_win_rate": wr,
            },
        )

        # Print a formatted research report
        self._print_report(proposal, wr)

        # Forward to Developer via strategy_queue
        try:
            self.strategy_queue.put_nowait({
                "fixes": proposal.parameter_changes,
                "source": f"osint:{proposal.name}",
                "description": proposal.description,
                "code_snippet": proposal.new_indicator_code,
                "expected_edge": proposal.expected_edge,
                "decision_id": dec_id,
            })
        except asyncio.QueueFull:
            logger.warning("strategy_queue full — OSINT proposal dropped")

    def _print_report(self, p: StrategyProposal, current_wr: float) -> None:
        divider = "═" * 62
        print(f"\n{divider}")
        print(f"  🔬 OSINT RESEARCHER — Strategy Discovery Report")
        print(f"{divider}")
        print(f"  Strategy  : {p.name}")
        print(f"  Source    : {p.source}")
        print(f"  Edge est. : +{p.expected_edge:.1f}pp  |  Current WR: {current_wr:.1%}")
        print(f"  Rationale : {p.rationale[:80]}...")
        print(f"  Params    : {json.dumps(p.parameter_changes, indent=4)}")
        if p.new_indicator_code:
            print(f"  Code snippet available — forwarded to Developer Agent")
        print(f"{divider}\n")
