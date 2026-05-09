# Polymarket CLOB V2 Cutover Runbook

**Cutover window:** 2026-04-28 ~11:00 UTC  
**Migration status: COMPLETE** — V2 live, bot running on `clob.polymarket.com`.  
**Completed:** 2026-05-09 (smoke test passed, fee formula corrected post-cutover)

---

## 10. Post-Cutover Smoke Test Results (2026-05-09)

Smoke test `py scripts/v2_smoke.py` — all 14 checks passed:

```
[PASS] SDK >= 1.0.1           (1.0.1)
[PASS] Host is production V2  (https://clob.polymarket.com)
[PASS] Host is not staging
[PASS] POLY_API_KEY set
[PASS] ETH_PRIVATE_KEY set
[PASS] POLY_FUNDER_ADDRESS set
[PASS] L2 auth (GET balance)
[PASS] V2 CTF Exchange approved
[WARN] pUSD balance below $5  (account balance: $0.41 — top up before live trading)
[PASS] Active BTC 5-min market found
[PASS] get_clob_market_info returned fd
[PASS] taker_fee_rate > 0
[PASS] market accepting orders
[PASS] two tokens returned
[PASS] WS URL unchanged in V2

ALL CHECKS PASSED
```

`/version` endpoint confirms `{"version":2}` on `clob.polymarket.com`.

---

## 11. Surprises / Anomalies Found During Cutover

### CRITICAL (fixed): Fee formula was wrong

**What we found:** The `fd` response field `{"r": 0.07, "e": 1, "to": true}` is **not** `r / 10^e`.
The SDK's own `fees.py` reveals the actual formula:

```python
platform_fee_per_share = fd.r × (ask × (1 − ask)) ^ fd.e
```

This is a **price-dependent, non-linear** taker fee that peaks at 1.75% when ask=0.50 and
tapers to 0% at the extremes. Our original code misinterpreted it as a flat 0.70% fee,
underestimating taker costs by 2.5–3.5× for mid-range prices.

**Impact before fix:** EV calculations were over-optimistic by ~0.011 per share at ask=0.65.
A trade showing EV=0.035 might only yield EV=0.020 after real fees. Trades near the 3%
safety buffer could have been taken that should have been rejected.

**Fix applied (2026-05-09):**
- `market/state.py` — replaced `taker_fee_rate: float` with `fee_rate_r` + `fee_rate_e`
- `market/discovery.py` — stores raw `fd.r` and `fd.e` instead of computing a scalar rate
- `engine/fee_filter.py` — new `_taker_fee_per_share(ask, r, e)` helper; all EV functions updated
- `main.py` — all call sites updated to pass `state.fee_rate_r, state.fee_rate_e`

**Fee table at fd={r:0.07, e:1}:**

| Ask price | Fee/share | Fee % |
|---|---|---|
| 0.50 | 0.01750 | 1.75% |
| 0.60 | 0.01680 | 1.68% |
| 0.65 | 0.01593 | 1.59% |
| 0.70 | 0.01470 | 1.47% |
| 0.75 | 0.01313 | 1.31% |

### MINOR (noted): `get_fee_rate_bps` / `mbf` / `tbf` are V1 legacy fields

`/fee-rate` returns `{"base_fee": 1000}` and `get_fee_rate_bps()` returns 1000.
These match `mbf` and `tbf` in the `clob-markets` response — all V1-era basis-point
fields that the V2 SDK **does not use** for V2 order construction
(`fee_rate_bps = None` when version == 2). Safe to ignore; do not use these for EV gating.

### MINOR (noted): `clob-v2.polymarket.com` issues HTTP 301

The staging hostname redirects to `clob.polymarket.com`. The SDK does not follow 301s —
any code pointing at the staging URL fails silently on authenticated POST requests.
Production default in `config.py` is already corrected to `clob.polymarket.com`.

### INFO: Builder attribution N/A

`POLY_BUILDER_CODE` is not set — no builder code configured. Attribution tracking on
`builders.polymarket.com` is not active. To enable: generate a bytes32 code at
`polymarket.com/settings?tab=builder` and set `POLY_BUILDER_CODE=0x...` in `.env`.

### INFO: `tick_size` must be a string in `PartialCreateOrderOptions`

`TickSize` is `Literal['0.1', '0.01', '0.001', '0.0001']` — always pass a string, never a float.
`PartialCreateOrderOptions()` (no argument) is safe — the SDK auto-resolves from market.
The `order_manager.py` already does this correctly.

---

## 1. SDK Version Check

The bot runs `py-clob-client-v2==1.0.1` (latest on PyPI as of 2026-05-09).

To check for newer patches:

```
pip show py-clob-client-v2
pip index versions py-clob-client-v2
pip install --upgrade py-clob-client-v2
```

Pin the new version in `requirements.txt` before restarting.

---

## 2. Host Configuration

The CLOB host is controlled by a **single env var**:

```
POLYMARKET_CLOB_HOST=https://clob.polymarket.com   # V2 production (default)
```

Code path: [config.py:74](config.py#L74)

```python
POLY_HOST: str = os.environ.get("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com")
```

> `clob-v2.polymarket.com` issued HTTP 301 during staging and is now decommissioned.
> Use `clob.polymarket.com` only. The default is already correct — do not set the env var
> unless you need to override it.

---

## 3. Pre-Cutover Checklist *(historical — cutover complete)*

```
[x] pip show py-clob-client-v2     → 1.0.1
[x] .env has POLYMARKET_CLOB_HOST set to clob.polymarket.com (or absent)
[x] .env has DRY_RUN=true
[x] No open limit orders           → confirmed, 5-min markets expire by design
[x] Smoke test passes              → py scripts/v2_smoke.py  (all 14 checks)
[x] /version returns {"version":2}
```

---

## 4. Graceful Drain *(historical)*

No standing orders to drain — the sniper only places orders in the last 10–15 seconds
of each 5-minute market, which expire or are cancelled within that window.

To drain (if ever needed):

```python
from py_clob_client_v2.client import ClobClient
# ... (see RUNBOOK §4 inline snippet)
orders = client.cancel_all()
print("cancel_all result:", orders)
```

---

## 5. Open Positions / Standing Orders After Cutover

This bot uses **5-minute expiry markets exclusively** — no GTC orders, no market-making.

**Confirmed at cutover: zero open orders, zero open positions.**

If a live trade is ever in-flight when the bot is stopped:
- Check `paper_trades.csv` for `PENDING` rows → `py main.py --resolve`
- Verify on-chain via [polygonscan.com](https://polygonscan.com)

---

## 6. Rollback

**There is no V1 fallback.** V1 is decommissioned.

If V2 misbehaves:
1. `Ctrl+C` — stop the bot immediately.
2. Keep `DRY_RUN=true` until V2 is confirmed stable.
3. Check [Polymarket status](https://status.polymarket.com).
4. File a bug via Polymarket Discord `#dev-support` with the full log.
5. Wait for a Polymarket hotfix. The bot can be left stopped indefinitely.

---

## 7. Running the Smoke Test

```
py scripts/v2_smoke.py
```

Checks: SDK version, host, L2 auth, balance, V2 CTF allowance, `get_clob_market_info`
(fd, mts, mos, ao, tokens), and WebSocket URL. Exits 0 on pass, 1 on any failure.

---

## 8. Key File Reference

| File | Role | V2 change |
|---|---|---|
| `config.py` | All env var defaults | `POLYMARKET_CLOB_HOST` → `clob.polymarket.com` |
| `execution/order_manager.py` | Order placement | V2 `OrderArgs`, no `fee_rate_bps` |
| `market/discovery.py` | Market discovery + fee fetch | `_fetch_market_fee_params()` stores `fd.r`, `fd.e` |
| `engine/fee_filter.py` | EV gating | Dynamic fee formula `fd.r × (p(1-p))^fd.e` |
| `market/state.py` | Runtime state | `fee_rate_r`, `fee_rate_e` fields (replaces `taker_fee_rate`) |
| `requirements.txt` | SDK pin | `py-clob-client-v2==1.0.1` |
| `scripts/v2_smoke.py` | Post-cutover sanity check | (new) |
