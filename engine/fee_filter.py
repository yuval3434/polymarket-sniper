import engine.strategy_loader as cfg
from config import MAKER_REBATE_RATE, TAKER_FEE_RATE


def calculate_obi(
    bid_depth_usdc: float,
    ask_depth_usdc: float,
) -> float:
    """
    Order Book Imbalance (OBI) = bid_depth / (bid_depth + ask_depth).

    Interpretation:
      OBI > 0.5  — more buying pressure than selling (bullish book)
      OBI < 0.5  — more selling pressure (bearish book / toxic flow)
      OBI = 0.0  — no bid depth at all (extremely dangerous to enter)

    Called with the cumulative USDC depth within the slippage band on each side.
    Returns 0.5 if both sides are zero (neutral / unknown).
    """
    total = bid_depth_usdc + ask_depth_usdc
    if total <= 0:
        return 0.5   # unknown — treat as neutral, don't block on missing data
    return bid_depth_usdc / total


def expected_value(p_true: float, ask_price: float, is_maker: bool = False) -> float:
    """
    EV of buying one share at ask_price given true win probability p_true.
    Fee rates are fixed constants (regulatory); thresholds come from strategy_config.json.
    """
    fee = -MAKER_REBATE_RATE * ask_price if is_maker else TAKER_FEE_RATE * ask_price
    return p_true * (1.0 - ask_price - fee) + (1.0 - p_true) * -(ask_price + fee)


def passes_fee_filter(
    p_true: float,
    ask_price: float,
    best_bid: float,
    best_ask: float,
    bid_depth_usdc: float = 0.0,
    ask_depth_usdc: float = 0.0,
) -> tuple[bool, bool]:
    """
    Decide whether to execute and whether to use maker or taker order.
    All thresholds are read live from strategy_config.json.

    New optional args:
      bid_depth_usdc / ask_depth_usdc — cumulative USDC liquidity within the
      slippage band on each side. Used for OBI check when enabled.

    Returns:
        (should_execute, use_maker)
    """
    safety  = cfg.safety_buffer()
    max_ask = cfg.max_ask_price()
    spread_thresh = cfg.maker_spread_threshold()

    # Hard reject: above max ask leaves no margin after fees
    if ask_price >= max_ask:
        return False, False

    # ── OBI filter: block toxic order flow ───────────────────────────────────
    if cfg.use_obi_filter() and (bid_depth_usdc > 0 or ask_depth_usdc > 0):
        obi = calculate_obi(bid_depth_usdc, ask_depth_usdc)
        min_obi = cfg.obi_min_ratio()
        if obi < min_obi:
            import logging
            logging.getLogger(__name__).warning(
                "OBI FILTER: trade blocked — OBI=%.3f < min=%.3f "
                "(bid_depth=$%.2f ask_depth=$%.2f) — toxic sell-side pressure detected",
                obi, min_obi, bid_depth_usdc, ask_depth_usdc,
            )
            return False, False

    spread    = best_ask - best_bid
    use_maker = spread > spread_thresh

    ev = expected_value(p_true, ask_price, is_maker=use_maker)
    if ev <= safety:
        ev_taker = expected_value(p_true, ask_price, is_maker=False)
        if ev_taker <= safety:
            return False, False
        use_maker = False

    # Raw margin guard — catches edge cases where EV scrapes through
    if 1.0 - ask_price * (1 + TAKER_FEE_RATE) <= safety:
        return False, False

    return True, use_maker
