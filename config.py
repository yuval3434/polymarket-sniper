import os
from dotenv import load_dotenv

load_dotenv()

# --- Polymarket CLOB credentials ---
POLY_API_KEY: str = os.environ.get("POLY_API_KEY", "")
POLY_SECRET: str = os.environ.get("POLY_SECRET", "")
POLY_PASSPHRASE: str = os.environ.get("POLY_PASSPHRASE", "")
POLY_FUNDER_ADDRESS: str = os.environ.get("POLY_FUNDER_ADDRESS", "")
ETH_PRIVATE_KEY: str = os.environ.get("ETH_PRIVATE_KEY", "")

# --- Trading parameters ---
DRY_RUN: bool = os.environ.get("DRY_RUN", "true").lower() == "true"
MAX_POSITION_USDC: float = float(os.environ.get("MAX_POSITION_USDC", "10.0"))
SAFETY_BUFFER: float = float(os.environ.get("SAFETY_BUFFER", "0.03"))

# --- Strategy parameters ---
ORACLE_LAG_SECONDS: float = float(os.environ.get("ORACLE_LAG_SECONDS", "2.0"))
SNIPE_WINDOW_START: int = int(os.environ.get("SNIPE_WINDOW_START", "12"))
KILL_SWITCH_LATENCY_MS: int = int(os.environ.get("KILL_SWITCH_LATENCY_MS", "3000"))

# --- Network endpoints ---
POLY_HOST: str = os.environ.get("POLY_HOST", "https://clob.polymarket.com")
GAMMA_API_BASE: str = os.environ.get("GAMMA_API_BASE", "https://gamma-api.polymarket.com")
BINANCE_WS_URL: str = os.environ.get(
    "BINANCE_WS_URL", "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
)
POLY_WS_URL: str = os.environ.get(
    "POLY_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market"
)

# --- Fee constants (Polymarket global crypto taker fee) ---
TAKER_FEE_RATE: float = 0.018   # 1.8%
MAKER_REBATE_RATE: float = 0.002  # 0.2%

# --- Reconnect backoff (seconds) ---
RECONNECT_BACKOFF_INITIAL: float = 0.5
RECONNECT_BACKOFF_MAX: float = 4.0

# --- Volatility window (seconds of Binance trades to keep) ---
VOL_WINDOW_SECONDS: int = 30

# --- Paper trade output ---
PAPER_TRADE_CSV: str = "paper_trades.csv"
