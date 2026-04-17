import engine.strategy_loader as cfg
from config import MAKER_REBATE_RATE, TAKER_FEE_RATE


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
) -> tuple[bool, bool]:
    """
    Decide whether to execute and whether to use maker or taker order.
    All thresholds are read live from strategy_config.json.

    Returns:
        (should_execute, use_maker)
    """
    safety  = cfg.safety_buffer()
    max_ask = cfg.max_ask_price()
    spread_thresh = cfg.maker_spread_threshold()

    # Hard reject: above max ask leaves no margin after fees
    if ask_price >= max_ask:
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
