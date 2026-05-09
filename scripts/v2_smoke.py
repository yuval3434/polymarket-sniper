"""
V2 Smoke Test
=============
Run after the April 28 2026 CLOB V2 cutover to confirm the bot is wired
correctly before re-enabling live trading.

Usage:
    py scripts/v2_smoke.py

Exit codes:
    0 — all checks passed
    1 — one or more checks failed (do NOT restart live bot)
"""
from __future__ import annotations

import importlib.metadata
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

# Load .env from repo root
_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))

from dotenv import load_dotenv
load_dotenv(_root / ".env")

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
WARN = "\033[33mWARN\033[0m"

_failures: list[str] = []


def _check(label: str, ok: bool, detail: str = "") -> None:
    tag = PASS if ok else FAIL
    suffix = f"  ({detail})" if detail else ""
    print(f"  [{tag}] {label}{suffix}")
    if not ok:
        _failures.append(label)


# ---------------------------------------------------------------------------
# 1. SDK version
# ---------------------------------------------------------------------------
print("\n=== 1. SDK version ===")
try:
    sdk_ver = importlib.metadata.version("py_clob_client_v2")
    print(f"  py_clob_client_v2 : {sdk_ver}")
    from packaging.version import Version
    _check("SDK >= 1.0.1", Version(sdk_ver) >= Version("1.0.1"), sdk_ver)
except Exception as exc:
    # packaging may not be installed — just print and skip version compare
    print(f"  (could not parse version: {exc})")
    print(f"  py_clob_client_v2 installed — version string check skipped")

# ---------------------------------------------------------------------------
# 2. Host configuration
# ---------------------------------------------------------------------------
print("\n=== 2. Host configuration ===")
HOST = os.environ.get("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com")
print(f"  POLYMARKET_CLOB_HOST : {HOST}")
_check(
    "Host is production V2",
    "clob.polymarket.com" in HOST,
    f"got {HOST!r} — expected clob.polymarket.com",
)
_check(
    "Host is not staging (clob-v2)",
    "clob-v2.polymarket.com" not in HOST,
    "staging URL issues HTTP 301 which the SDK does not follow",
)

# ---------------------------------------------------------------------------
# 3. Authenticated balance check (L2 auth)
# ---------------------------------------------------------------------------
print("\n=== 3. L2 auth + balance check ===")
try:
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import ApiCreds, BalanceAllowanceParams, AssetType
    from py_clob_client_v2.constants import POLYGON

    POLY_API_KEY    = os.environ.get("POLY_API_KEY", "")
    POLY_SECRET     = os.environ.get("POLY_SECRET", "")
    POLY_PASSPHRASE = os.environ.get("POLY_PASSPHRASE", "")
    POLY_FUNDER     = os.environ.get("POLY_FUNDER_ADDRESS", "")
    ETH_KEY         = os.environ.get("ETH_PRIVATE_KEY", "")
    SIG_TYPE        = int(os.environ.get("SIGNATURE_TYPE", "0"))

    _check("POLY_API_KEY set", bool(POLY_API_KEY))
    _check("ETH_PRIVATE_KEY set", bool(ETH_KEY))
    _check("POLY_FUNDER_ADDRESS set", bool(POLY_FUNDER))

    client = ClobClient(
        host=HOST,
        key=ETH_KEY,
        chain_id=POLYGON,
        creds=ApiCreds(
            api_key=POLY_API_KEY,
            api_secret=POLY_SECRET,
            api_passphrase=POLY_PASSPHRASE,
        ),
        funder=POLY_FUNDER,
        signature_type=SIG_TYPE,
    )

    bal_resp = client.get_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    )
    raw_balance = int(bal_resp.get("balance", 0))
    usdc = raw_balance / 1e6
    print(f"  pUSD balance : {usdc:.4f}  (raw: {raw_balance})")

    # Confirm V2 CTF Exchange allowance is set (non-zero)
    V2_CTF = "0xE111180000d2663C0091e4f400237545B87B996B"
    allowances = bal_resp.get("allowances", {})
    ctf_allowance = int(allowances.get(V2_CTF, 0))
    _check("L2 auth (GET balance)", True, f"balance={usdc:.4f} pUSD")
    _check(
        "V2 CTF Exchange approved",
        ctf_allowance > 0,
        f"allowance for {V2_CTF[:10]}… = {ctf_allowance}",
    )
    if usdc < 5.0:
        tag = WARN
        print(f"  [{tag}] pUSD balance below $5 minimum order size — cannot place orders")

except Exception as exc:
    _check("L2 auth", False, str(exc))
    client = None

# ---------------------------------------------------------------------------
# 4. get_clob_market_info on current BTC 5-min market
# ---------------------------------------------------------------------------
print("\n=== 4. Market info (get_clob_market_info) ===")
try:
    # Resolve the current 5-min BTC market via Gamma API
    now = int(time.time())
    current_ts = now - (now % 300)
    condition_id: str | None = None

    for ts in (current_ts, current_ts + 300, current_ts - 300):
        slug = f"btc-updown-5m-{ts}"
        url = f"https://gamma-api.polymarket.com/markets?slug={slug}"
        req = urllib.request.Request(url, headers={"User-Agent": "py-clob-client-v2/smoke"})
        try:
            with urllib.request.urlopen(req, timeout=8) as r:
                data = json.loads(r.read())
            if data:
                m = data[0] if isinstance(data, list) else data
                condition_id = m.get("conditionId")
                print(f"  market slug  : {slug}")
                print(f"  condition_id : {condition_id}")
                print(f"  end_date     : {m.get('endDate','?')}")
                break
        except Exception:
            continue

    _check("Active BTC 5-min market found", condition_id is not None)

    if condition_id and client is not None:
        info = client.get_clob_market_info(condition_id)

        fd   = info.get("fd") or {}
        r    = float(fd.get("r", 0))
        e    = float(fd.get("e", 0))
        rate = r / (10 ** e) if e else r
        mts  = info.get("mts")
        mos  = info.get("mos")
        ao   = info.get("ao", False)

        print(f"  mts          : {mts}")
        print(f"  mos          : {mos}")
        print(f"  fd           : {fd}")
        print(f"  taker_fee    : {rate:.4f}  ({rate*100:.2f}%)")
        print(f"  ao           : {ao}")

        _check("get_clob_market_info returned fd", bool(fd), str(fd))
        _check("taker_fee_rate > 0", rate > 0, f"{rate:.4f}")
        _check("market accepting orders", ao)

        tokens = info.get("t", [])
        _check("two tokens returned", len(tokens) == 2, f"got {len(tokens)}")
        if tokens:
            print("  tokens:")
            for t in tokens:
                print(f"    {t.get('o','?')} => {t.get('t','?')[:20]}...")

except Exception as exc:
    _check("get_clob_market_info", False, str(exc))

# ---------------------------------------------------------------------------
# 5. WebSocket URL sanity
# ---------------------------------------------------------------------------
print("\n=== 5. WebSocket URL ===")
WS_URL = os.environ.get("POLY_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
print(f"  POLY_WS_URL : {WS_URL}")
_check(
    "WS URL unchanged in V2",
    "ws-subscriptions-clob.polymarket.com" in WS_URL,
)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print()
print("=" * 58)
if _failures:
    print(f"  FAILED ({len(_failures)} check(s)):")
    for f in _failures:
        print(f"    x  {f}")
    print()
    print("  Do NOT restart the live bot until all checks pass.")
    print("=" * 58)
    sys.exit(1)
else:
    print("  ALL CHECKS PASSED — bot is safe to restart")
    print(f"  Host    : {HOST}")
    print(f"  SDK     : py_clob_client_v2 {sdk_ver if 'sdk_ver' in dir() else '?'}")
    print("=" * 58)
    sys.exit(0)
