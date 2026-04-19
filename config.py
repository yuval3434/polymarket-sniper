import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# --- Polymarket CLOB credentials ---
POLY_API_KEY: str = os.environ.get("POLY_API_KEY", "")
POLY_SECRET: str = os.environ.get("POLY_SECRET", "")
POLY_PASSPHRASE: str = os.environ.get("POLY_PASSPHRASE", "")
POLY_FUNDER_ADDRESS: str = os.environ.get("POLY_FUNDER_ADDRESS", "")
ETH_PRIVATE_KEY: str = os.environ.get("ETH_PRIVATE_KEY", "")
SIGNATURE_TYPE: int = int(os.environ.get("SIGNATURE_TYPE", "0"))

# --- Trading parameters ---
# NOTE: DRY_RUN is the env-file default only.
# main.py --mode live/paper overrides this at runtime.
DRY_RUN: bool = os.environ.get("DRY_RUN", "true").lower() == "true"


def validate_live_credentials() -> None:
    """
    Called before live mode starts.
    Checks every required secret is present and well-formed.
    Calls sys.exit(1) with a CRITICAL log on any failure — never
    allow live trading to start with missing/malformed credentials.
    """
    errors: list[str] = []

    if not POLY_API_KEY:
        errors.append("POLY_API_KEY is missing")
    if not POLY_SECRET:
        errors.append("POLY_SECRET is missing")
    if not POLY_PASSPHRASE:
        errors.append("POLY_PASSPHRASE is missing")
    if not POLY_FUNDER_ADDRESS:
        errors.append("POLY_FUNDER_ADDRESS is missing")
    elif not POLY_FUNDER_ADDRESS.startswith("0x") or len(POLY_FUNDER_ADDRESS) != 42:
        errors.append(
            f"POLY_FUNDER_ADDRESS looks malformed (got '{POLY_FUNDER_ADDRESS[:10]}…')"
            " — expected 0x + 40 hex chars"
        )
    if not ETH_PRIVATE_KEY:
        errors.append("ETH_PRIVATE_KEY is missing")
    elif not ETH_PRIVATE_KEY.startswith("0x"):
        errors.append("ETH_PRIVATE_KEY must start with '0x'")

    if errors:
        logger.critical("=" * 62)
        logger.critical("LIVE MODE CREDENTIAL CHECK FAILED — cannot start")
        for e in errors:
            logger.critical("  ✗  %s", e)
        logger.critical("Fix your .env file and restart.")
        logger.critical("=" * 62)
        sys.exit(1)

    logger.info("Live credential check passed ✓  (API key …%s)", POLY_API_KEY[-4:])
MAX_POSITION_USDC: float = float(os.environ.get("MAX_POSITION_USDC", "10.0"))
STARTING_BANK_USDC: float = float(os.environ.get("STARTING_BANK_USDC", "100.0"))
SAFETY_BUFFER: float = float(os.environ.get("SAFETY_BUFFER", "0.03"))

# --- Strategy parameters ---
ORACLE_LAG_SECONDS: float = float(os.environ.get("ORACLE_LAG_SECONDS", "2.0"))
SNIPE_WINDOW_START: int = int(os.environ.get("SNIPE_WINDOW_START", "12"))
KILL_SWITCH_LATENCY_MS: int = int(os.environ.get("KILL_SWITCH_LATENCY_MS", "60000"))

# --- Network endpoints ---
POLY_HOST: str = os.environ.get("POLY_HOST", "https://clob.polymarket.com")
GAMMA_API_BASE: str = os.environ.get("GAMMA_API_BASE", "https://gamma-api.polymarket.com")
BINANCE_WS_URL: str = os.environ.get(
    "BINANCE_WS_URL", "wss://stream.binance.us:9443/ws/btcusdt@miniTicker"
)
POLYGON_RPC_URL: str = os.environ.get(
    "POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com"
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

# --- Cross-machine trade sync ---
# Comma-separated list of peer bot URLs, e.g. "http://1.2.3.4:8765,http://5.6.7.8:8765"
REMOTE_BOT_PEERS: list[str] = [
    p.strip() for p in os.environ.get("REMOTE_BOT_PEERS", "").split(",") if p.strip()
]
TRADE_SERVER_PORT: int = int(os.environ.get("TRADE_SERVER_PORT", "8765"))
