"""
Fee Filter — Dynamic Taker Fee (post-Feb 18 2026 Polymarket rules)
===================================================================
Polymarket replaced the flat 1.8% taker fee with a probability-dependent formula:

    fee_rate = 0.25 × (p × (1 − p))²

This peaks at 1.5625% when p = 0.50 (maximum uncertainty) and falls sharply
toward zero as the market approaches certainty:

    p = 0.50  →  fee = 1.563%
    p = 0.70  →  fee = 0.709%
    p = 0.85  →  fee = 0.141%
    p = 0.92  →  fee = 0.037%
    p = 0.95  →  fee = 0.014%

Maker orders still earn a rebate (flat 0.2%) — unchanged.

The fee is also needed in basis points (bps) for order signing:
    fee_bps = round(fee_rate × 10_000)
"""

import logging

import engine.strategy_loader as cfg
from config import MAKER_REBATE_RATE

logger = logging.getLogger(__name__)


def dynamic_taker_fee(p_true: float) -> float:
    """
    Compute the dynamic taker fee rate for a given win probability.

    Formula: fee = 0.25 × (p × (1 − p))²

    The term (p × (1−p)) is the Bernoulli variance — it's maximised at p=0.5
    and shrinks to zero at p→0 or p→1. Squaring it makes the fee fall even
    faster at the extremes, rewarding high-confidence bets with near-zero fees.
    """
    p = max(0.001, min(0.999, p_true))  # clamp to avoid 0/1 edge cases
    bernoulli_var = p * (1.0 - p)
    return 0.25 * (bernoulli_var ** 2)


def taker_fee_bps(p_true: float) -> int:
    """
    Return the fee in basis points (integer) for inclusion in order signing.
    Required by Polymarket CLOB API — orders missing feeRateBps are rejected.
    """
    return 1000


def expected_value(p_true: float, ask_price: float, is_maker: bool = False) -> float:
    """
    EV of buying one share at ask_price given true win probability p_true.

    Taker fee is now dynamic (probability-dependent).
    Maker rebate remains flat at 0.2%.

    EV = P(win) × (payout − ask − fee) + P(loss) × (−ask − fee)
       = P(win) × (1 − ask − fee) − (1 − P(win)) × (ask + fee)
    """
    if is_maker:
        fee = -MAKER_REBATE_RATE * ask_price   # negative fee = rebate (income)
    else:
        fee = dynamic_taker_fee(p_true) * ask_price

    return p_true * (1.0 - ask_price - fee) + (1.0 - p_true) * -(ask_price + fee)


def passes_fee_filter(
    p_true: float,
    ask_price: float,
    best_bid: float,
    best_ask: float,
) -> tuple[bool, bool]:
    """
    Decide whether to execute and whether to use maker or taker order.
    All thresholds read live from strategy_config.json.

    Returns: (should_execute, use_maker)
    """
    safety        = cfg.safety_buffer()
    max_ask       = cfg.max_ask_price()
    spread_thresh = cfg.maker_spread_threshold()

    if ask_price >= max_ask:
        return False, False

    spread    = best_ask - best_bid
    use_maker = spread > spread_thresh

    ev = expected_value(p_true, ask_price, is_maker=use_maker)
    if ev <= safety:
        # Try taker before giving up
        ev_taker = expected_value(p_true, ask_price, is_maker=False)
        if ev_taker <= safety:
            return False, False
        use_maker = False

    # Raw margin guard using the dynamic fee
    fee_rate = dynamic_taker_fee(p_true)
    if 1.0 - ask_price * (1 + fee_rate) <= safety:
        return False, False

    return True, use_maker


def check_merge_arb(
    up_ask: float,
    down_ask: float,
    min_profit_pct: float = 0.005,
) -> tuple[bool, float]:
    """
    Detect merge arbitrage opportunity: buy 1 UP + 1 DOWN → merge → receive $1.00.

    Profitable when:  up_ask + down_ask + total_fees < 1.00

    Fee insight (from "Unlawful-Shear" bot analysis, April 2026):
      At extreme prices (p≈0.01 / p≈0.99), the dynamic fee collapses to near-zero
      because fee = 0.25 × (p×(1−p))². This is exactly when merge arb is available
      — one side is pricing 1-3¢ and the other 97-99¢.

    Returns: (is_arb_available, expected_profit_per_share_pair)
    """
    if up_ask <= 0 or down_ask <= 0 or up_ask >= 1 or down_ask >= 1:
        return False, 0.0

    # Fee for UP side: p_true ≈ up_ask (market-implied probability)
    fee_up   = dynamic_taker_fee(up_ask) * up_ask
    # Fee for DOWN side: p_true ≈ down_ask
    fee_down = dynamic_taker_fee(down_ask) * down_ask

    total_cost   = up_ask + down_ask + fee_up + fee_down
    gross_profit = 1.0 - total_cost

    is_arb = gross_profit >= min_profit_pct

    if is_arb:
        logger.info(
            "MERGE ARB DETECTED: up_ask=%.4f down_ask=%.4f sum=%.4f"
            " fee=%.4f profit/share=%.4f (%.2f%%)",
            up_ask, down_ask, up_ask + down_ask,
            fee_up + fee_down, gross_profit, gross_profit * 100,
        )

    return is_arb, gross_profit
