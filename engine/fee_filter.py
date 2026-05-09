"""
Fee Filter — V2 SDK-sourced Taker Fees
=======================================
In V2, the taker fee for each market is price-dependent. The SDK fetches two
parameters from get_clob_market_info(condition_id):

    fd = {"r": 0.07, "e": 1, "to": True}

The per-share taker fee at a given ask price is:

    fee_per_share = fd.r × (ask × (1 − ask)) ^ fd.e

e.g. fd={r:0.07, e:1} at ask=0.65: 0.07 × 0.65 × 0.35 = 0.01593/share (1.59%)
     same params at ask=0.50 (max): 0.07 × 0.25 = 0.0175/share (1.75%)

These coefficients are stored in MarketState.fee_rate_r and MarketState.fee_rate_e.
Makers pay zero fees in V2 — no rebate, no charge.

The fee math in these functions is for EV gating only. Order-level fee
adjustment is handled by the SDK's adjust_buy_amount_for_fees() internally.
"""

import logging

import engine.strategy_loader as cfg

logger = logging.getLogger(__name__)


def _taker_fee_per_share(ask_price: float, fee_rate_r: float, fee_rate_e: float) -> float:
    """Dynamic V2 taker fee per share at the given ask price."""
    if fee_rate_r <= 0 or ask_price <= 0 or ask_price >= 1:
        return 0.0
    return fee_rate_r * (ask_price * (1.0 - ask_price)) ** fee_rate_e


def expected_value(
    p_true: float,
    ask_price: float,
    fee_rate_r: float = 0.0,
    fee_rate_e: float = 0.0,
    is_maker: bool = False,
) -> float:
    """
    EV of buying one share at ask_price given true win probability p_true.

    V2: makers pay zero fees; takers pay a price-dependent fee:
        fee_per_share = fee_rate_r × (ask × (1 − ask)) ^ fee_rate_e

    EV = P(win) × (1 − ask − fee) − P(loss) × (ask + fee)
    """
    fee = 0.0 if is_maker else _taker_fee_per_share(ask_price, fee_rate_r, fee_rate_e)
    return p_true * (1.0 - ask_price - fee) + (1.0 - p_true) * -(ask_price + fee)


def passes_fee_filter(
    p_true: float,
    ask_price: float,
    best_bid: float,
    best_ask: float,
    fee_rate_r: float = 0.0,
    fee_rate_e: float = 0.0,
) -> tuple[bool, bool]:
    """
    Decide whether to execute and whether to use maker or taker order.
    All thresholds read live from strategy_config.json.

    fee_rate_r / fee_rate_e: V2 fee coefficients from MarketState.
    Makers always pay 0 in V2.

    Returns: (should_execute, use_maker)
    """
    safety        = cfg.safety_buffer()
    max_ask       = cfg.max_ask_price()
    spread_thresh = cfg.maker_spread_threshold()

    if ask_price >= max_ask:
        return False, False

    spread    = best_ask - best_bid
    use_maker = spread > spread_thresh

    ev = expected_value(p_true, ask_price, fee_rate_r, fee_rate_e, is_maker=use_maker)
    if ev <= safety:
        ev_taker = expected_value(p_true, ask_price, fee_rate_r, fee_rate_e, is_maker=False)
        if ev_taker <= safety:
            return False, False
        use_maker = False

    # Raw margin guard: ensure gross payout covers ask + max taker fee (at p=0.5)
    max_fee = _taker_fee_per_share(0.5, fee_rate_r, fee_rate_e)
    if 1.0 - ask_price - max_fee <= safety:
        return False, False

    return True, use_maker


def check_merge_arb(
    up_ask: float,
    down_ask: float,
    fee_rate_r: float = 0.0,
    fee_rate_e: float = 0.0,
    min_profit_pct: float = 0.005,
) -> tuple[bool, float]:
    """
    Detect merge arbitrage: buy 1 UP + 1 DOWN → merge → receive $1.00.
    Profitable when: up_ask + down_ask + fees_both_legs < 1.00

    Uses dynamic V2 per-share fees for each leg.

    Returns: (is_arb_available, expected_profit_per_share_pair)
    """
    if up_ask <= 0 or down_ask <= 0 or up_ask >= 1 or down_ask >= 1:
        return False, 0.0

    fee_up   = _taker_fee_per_share(up_ask,   fee_rate_r, fee_rate_e)
    fee_down = _taker_fee_per_share(down_ask, fee_rate_r, fee_rate_e)

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
