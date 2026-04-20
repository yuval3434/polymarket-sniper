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

    # NOTE: snipe_fired guard removed in v1.9.1 — re-entry is governed by
    # position_manager.has_open_position, allowing multi-surf within one window.

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
        self.opening_price = self.btc_price  # snapshot Binance price at open
