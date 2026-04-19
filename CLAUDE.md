# Polymarket BTC Sniper — Multi-Agent System
## CLAUDE.md — Project Bible for Claude Code Sessions

---

## Project Overview

A self-optimising, autonomous multi-agent trading bot that executes positions
on Polymarket's 5-minute Bitcoin UP/DOWN prediction markets. The system:

- **Snipes** the final 10–15 seconds before market expiry
- **Manages positions** actively (take profit / stop loss)
- **Learns** from every trade via post-mortem analysis
- **Researches** new strategies and backtests them automatically
- **Promotes** only validated strategies after mandatory paper trading

---

## ⚠️ MANDATORY ARCHITECTURAL RULE — PAPER TRADING GATE

> **NO strategy change, parameter update, or code modification may be applied
> to the live bot without first completing the paper trading validation pipeline.**

The pipeline is strictly:

```
Proposal (Evaluator/OSINT)
    → Backtest (≥15 trades, ≥52% win rate)
        → stage_candidate() in learning_agent/developer.py
            → Paper trade validation (≥20 real trades AND ≥2 hours)
                → All 5 gates pass
                    → promote_candidate() replaces strategy_config.json
```

**Never call `promote_candidate()` directly. Never modify `strategy_config.json` manually.**
**Never modify `.py` source files via agent code — only `strategy_config.json` is agent-writable.**

---

## Directory Structure

```
polymarket_sniper/
├── CLAUDE.md                   ← You are here
├── main.py                     ← Entry point (py main.py)
├── config.py                   ← Env vars + constants
├── strategy_config.json        ← LIVE strategy parameters (agent-managed)
├── candidate_config.json       ← Candidate under paper testing (auto-managed)
├── learning_history.json       ← Audit log of all experiments
├── trading_bot.db              ← SQLite: trades, ticks, decisions, performance
├── paper_trades.csv            ← Human-readable paper trade log
├── candidate_paper_trades.csv  ← Shadow trades for candidate validation
│
├── agents/                     ← Multi-agent system
│   ├── orchestrator.py         ← Supervises all agents + inter-agent queues
│   ├── position_manager.py     ← Active trade management (TP/SL/expiry)
│   ├── evaluator.py            ← Post-mortem + market pulse
│   ├── osint_researcher.py     ← Strategy discovery (hourly)
│   └── developer.py            ← Backtest → stage → paper trade → promote
│
├── core/                       ← Shared infrastructure
│   ├── database.py             ← SQLite CRUD (trades, ticks, decisions)
│   ├── pnl_tracker.py          ← Live P&L dashboard
│   └── risk_manager.py         ← Circuit breaker + Kelly bet sizing
│
├── engine/
│   ├── probability.py          ← Log-normal P(up/down) with oracle lag
│   └── fee_filter.py           ← EV + fee calculation, maker/taker routing
│
├── streams/
│   ├── binance_stream.py       ← Binance aggTrade WS (port 443, REST fallback)
│   └── polymarket_stream.py    ← Polymarket CLOB WS order book
│
├── market/
│   ├── state.py                ← Shared MarketState dataclass
│   └── discovery.py            ← 5-min BTC market discovery (Gamma API)
│
├── execution/
│   └── order_manager.py        ← ClobClient wrapper, maker→FAK fallback
│
├── monitor/
│   └── kill_switch.py          ← Feed staleness watchdog (3s threshold, soft/auto-recovery)
│
├── simulation/
│   └── paper_trader.py         ← CSV paper trade logger + resolver
│
└── learning_agent/
    ├── reviewer.py             ← Loss pattern analysis from CSV
    ├── researcher.py           ← Backtest engine (Binance klines)
    ├── developer.py            ← Atomic config writer / promoter
    ├── validator.py            ← Paper trade gate (5 gates)
    └── code_snippets/          ← OSINT code snippets (human review only)
```

---

## Terminal Run Commands

```bash
# ── Execution modes ───────────────────────────────────────────────────────
py main.py                   # paper trade (safe default — no real funds)
py main.py --mode paper      # explicit paper mode (identical to above)
py main.py --mode live       # LIVE trading — validates credentials, 5s countdown

# ── Live trading limits (HARD-CODED — never change without testing) ───────
# Minimum bet : $10 USDC per trade
# Maximum bet : $15 USDC per trade
# These clamps are enforced in core/risk_manager.py:apply_live_clamp()
# regardless of what Kelly sizing suggests. Any deviation is logged as WARNING.

# ── Utility commands ──────────────────────────────────────────────────────
py main.py --resolve         # score pending paper trades and exit
py main.py --status          # print session stats from DB and exit

# Install / update dependencies
py -m pip install -r requirements.txt

# ── Dashboard (separate process, safe to run alongside the bot) ────────────
streamlit run dashboard.py
# Opens at http://localhost:8501
# Reads trades_history.csv — zero impact on the trading loop
# Auto-refreshes every 15s when the checkbox is enabled

# View live SQLite performance data
sqlite3 trading_bot.db "SELECT * FROM performance ORDER BY id DESC LIMIT 5;"

# View recent trades
sqlite3 trading_bot.db "SELECT timestamp,token_side,entry_price,exit_price,exit_type,pnl_usd,win FROM trades ORDER BY id DESC LIMIT 20;"

# View agent decisions
sqlite3 trading_bot.db "SELECT timestamp,agent,decision_type,details FROM decisions ORDER BY id DESC LIMIT 20;"

# View learning history (promotions and rejections)
py -c "import json; [print(e['event'], e.get('experiment_id',''), e['ts']) for e in json.load(open('learning_history.json'))]"
```

---

## Environment Variables (.env)

```
POLY_API_KEY=...
POLY_SECRET=...
POLY_PASSPHRASE=...
POLY_FUNDER_ADDRESS=0x...
ETH_PRIVATE_KEY=0x...           # L1, used only to derive L2 creds

DRY_RUN=true                    # NEVER set false without reviewing paper results
MAX_POSITION_USDC=10.0
SAFETY_BUFFER=0.03
ORACLE_LAG_SECONDS=2.0
SNIPE_WINDOW_START=12
KILL_SWITCH_LATENCY_MS=3000
MAX_DRAWDOWN_HALT_USD=50.0      # Circuit breaker — halt if session loss > $50
MAX_DRAWDOWN_HALT_PCT=20.0      # Circuit breaker — halt if loss > 20% of bank
KELLY_FRACTION=0.25             # Quarter-Kelly for bet sizing
```

---

## Kill Switch — Auto-Recovery Behaviour (v1.1+)

The kill switch is **soft** — it pauses trading but never exits the process.

| Event | Action |
|---|---|
| Binance feed stale > 3s | `kill_switch_active=True`, `kill_switch_source="binance"`, `cancel_all()` called |
| Polymarket feed stale > 3s | Same, `kill_switch_source="polymarket"` |
| WS reconnects | Stream accumulates **3 consecutive valid ticks** |
| Buffer satisfied | `kill_switch_active=False`, logs `Kill Switch disengaged`, trading resumes |

**Binance reconnect backoff:** 1s → 2s → 4s → 8s → 10s max. After 3 WS failures, falls back to REST polling (200ms) until WS recovers.

**Polymarket feed** does not yet have a dedicated self-healing loop — if it goes stale the bot pauses but requires manual investigation.

---

## Agent Descriptions

| Agent | File | Trigger | Output |
|---|---|---|---|
| **PositionManager** | agents/position_manager.py | Every 500ms while position open | TP/SL/Expiry close → PnL display |
| **Evaluator** | agents/evaluator.py | Every closed trade | Post-mortem → strategy_queue |
| **OSINTResearcher** | agents/osint_researcher.py | Every 60 minutes | Strategy proposal → strategy_queue |
| **DeveloperAgent** | agents/developer.py | Every 2 proposals | Backtest → stage → promote/reject |
| **Orchestrator** | agents/orchestrator.py | Continuous | Supervises all agents |

---

## Key Tunable Parameters (strategy_config.json)

| Parameter | Default | Effect |
|---|---|---|
| `probability.oracle_lag_seconds` | 2.0 | Time added to τ for Chainlink lag |
| `sniper.window_open_seconds` | 290 | Entry window opens at T-290s (≈market open) |
| `sniper.window_close_seconds` | 90 | Hard cutoff — no new entries after T-90s |
| `sniper.early_cutoff_seconds` | 180 | Minutes 1-2 (tau>180) use lower Z threshold |
| `sniper.early_z_score` | 0.3 | Min Z for early momentum phase |
| `sniper.min_z_score` | 0.5 | Min Z for standard phase (minute 3) |
| `fee_filter.safety_buffer` | 0.03 | Minimum EV margin (3¢) |
| `fee_filter.max_ask_price` | 0.75 | Never buy above $0.75 (max entry price) |
| `indicators.use_momentum` | false | Require BTC momentum confirmation |

---

## Guidelines for Claude Code in Future Sessions

### When modifying strategy logic:
1. Always read `strategy_config.json` first — parameters live there, not in hardcoded values
2. Engine modules (`engine/probability.py`, `engine/fee_filter.py`) should read from config at call time
3. Never hardcode a parameter that is in `strategy_config.json`

### When the user reports a bug:
1. Check `trading_bot.db` decisions table first: `sqlite3 trading_bot.db "SELECT * FROM decisions ORDER BY id DESC LIMIT 10;"`
2. Check `paper_trades.csv` for recent outcomes
3. Check `learning_history.json` for recent promotions that may have caused regression

### When adding a new indicator:
1. Add the parameter to `strategy_config.json` under `indicators`
2. Implement the indicator function in `engine/`
3. Gate it behind `indicators.use_<name>: false` by default
4. Add a pattern detector in `learning_agent/reviewer.py` to enable it when relevant
5. Add a `StrategyProposal` entry in `agents/osint_researcher.py` knowledge base
6. Do NOT enable it in live config — let the paper trading pipeline promote it

### When adding a new agent:
1. Define its input queue and output queue
2. Register it in `agents/orchestrator.py` under `run_all()`
3. Wrap it in `_run_agent()` for auto-restart

### Safety checklist before any session ends:
- [ ] `DRY_RUN=true` in `.env` unless explicitly approved for live
- [ ] No uncommitted changes to `strategy_config.json` outside the pipeline
- [ ] Circuit breaker threshold has not been raised without justification
- [ ] `candidate_config.json` (if present) has been paper-traded ≥2h before promotion

---

## Data Flow Diagram

```
Binance aggTrade WS ──────────────────────────────►  MarketState.btc_price
                                                           │
Polymarket CLOB WS  ──────────────────────────────►  MarketState.{up,down}_best_{ask,bid}
                                                           │
Market Discovery (Gamma API, every 5min) ──────────►  MarketState.{token_ids, expiry_ts}
                                                           │
                                                    sniper_loop (100ms tick)
                                                           │
                           ┌───────────────────────────────┤
                           ▼                               ▼
                   passes_fee_filter()           orchestrator.on_snipe_signal()
                           │                               │
                    log_decision()                 ┌───────┴──────────┐
                    (paper_trades.csv)             ▼                  ▼
                                          PositionManager    developer.record_shadow_trade()
                                          (TP/SL monitor)             │
                                                   │          [candidate validation]
                                          outcome_queue
                                                   │
                                               Evaluator
                                          (post-mortem, pulse)
                                                   │
                                          strategy_queue
                                                   │
                                    ┌──────────────┴──────────────┐
                                    ▼                             ▼
                             OSINT Researcher               DeveloperAgent
                             (hourly proposals)       (backtest → stage → gate → promote)
```
