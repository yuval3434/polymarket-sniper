import math

from scipy.stats import norm  # type: ignore

import engine.strategy_loader as cfg
from market.state import MarketState


def calculate_probability(state: MarketState) -> tuple[float, float, float]:
    """
    Compute P(price above strike) and P(price below strike) at Chainlink
    resolution time using a log-normal terminal probability model.

      Z = ln(S/K) / (σ * sqrt(τ))
      P_up   = N(Z)
      P_down = 1 - P_up

    Parameters are read live from strategy_config.json (10-second TTL cache)
    so a promoted config takes effect without restarting the bot.

    Returns:
        (P_up, P_down, abs_Z)  — abs_Z is the confidence proxy used by
        the sniper loop's min_z_score gate.
    """
    S = state.btc_price
    K = state.opening_price

    if S <= 0 or K <= 0:
        return 0.5, 0.5, 0.0

    oracle_lag = cfg.oracle_lag_seconds()
    min_vol    = cfg.min_vol_floor()

    tau_raw  = state.seconds_to_expiry()
    tau      = max(0.1, tau_raw + oracle_lag)
    tau_yrs  = tau / (365 * 24 * 3600)

    sigma    = getattr(state, "_vol_estimate", 0.0) or min_vol
    sigma    = max(sigma, min_vol)

    sigma_tau = sigma * math.sqrt(tau_yrs)
    if sigma_tau <= 0:
        sigma_tau = 1e-9

    Z      = math.log(S / K) / sigma_tau
    P_up   = float(norm.cdf(Z))
    P_down = 1.0 - P_up

    return P_up, P_down, abs(Z)
