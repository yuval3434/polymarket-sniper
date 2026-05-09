import asyncio
import time
from dataclasses import dataclass, field


@dataclass
class MarketState:
    """
    Single source of truth for all runtime data.
    All fields updated by streams; read by the sniper engine.
    asyncio.Lock guards multi-field atomic updates.
    """
    # Market identity (populated by discovery)
    market_id: str = ""
    condition_id: str = ""
    up_token_id: str = ""
    down_token_id: str = ""
    opening_price: float = 0.0      # BTC price at the start of this 5-min window
    expiry_ts: float = 0.0          # Unix timestamp when market closes

    # Binance price stream
    btc_price: float = 0.0
    # Initialised 15s in the future so the kill switch doesn't fire before streams connect
    btc_last_update: float = field(default_factory=lambda: time.monotonic() + 15)

    # Polymarket order book (best ask for each side)
    up_best_ask: float = 1.0
    down_best_ask: float = 1.0
    up_best_bid: float = 0.0
    down_best_bid: float = 0.0
    poly_last_update: float = field(default_factory=lambda: time.monotonic() + 15)

    # Circuit breaker (soft — auto-recoverable)
    kill_switch_active: bool = False
    kill_switch_reason: str = ""
    kill_switch_source: str = ""   # "binance" | "polymarket" — used by stream loops for targeted recovery

    # V2 fee parameters (fetched from SDK after discovery via get_clob_market_info).
    # The actual per-share taker fee is price-dependent:
    #   fee_per_share = fee_rate_r * (ask * (1 - ask)) ** fee_rate_e
    # e.g. fd={"r":0.07,"e":1} at ask=0.65: 0.07*0.65*0.35 = 0.01593/share (1.59%)
    # Makers pay zero in V2.
    fee_rate_r: float = 0.0   # fd.r  — scaling coefficient
    fee_rate_e: float = 0.0   # fd.e  — price-sensitivity exponent

    # Snipe guard — prevent firing more than once per market window
    snipe_fired: bool = False

    def is_ready(self) -> bool:
        return (
            bool(self.up_token_id)
            and self.btc_price > 0
            and self.expiry_ts > 0
            and not self.kill_switch_active
        )

    def seconds_to_expiry(self) -> float:
        return self.expiry_ts - time.time()

    def reset_for_new_market(self) -> None:
        self.up_best_ask = 1.0
        self.down_best_ask = 1.0
        self.up_best_bid = 0.0
        self.down_best_bid = 0.0
        self.snipe_fired = False
        self.opening_price = self.btc_price  # snapshot Binance price at open
