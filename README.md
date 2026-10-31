# Polymarket BTC Sniper

An async, multi-agent trading system for Polymarket's 5-minute Bitcoin up/down markets. It streams live BTC trades from Binance and the Polymarket order book, estimates the probability that the market resolves up or down, and trades in the final seconds before expiry only when the expected value beats the fees.

On top of the trading loop sits a learning pipeline that reviews closed trades, proposes parameter changes, backtests them, and paper-trades them before anything reaches the live bot.

## How it decides

**Probability model** — a log-normal model of where BTC will be at resolution:

```
Z      = ln(S / K) / (σ · √τ)
P(up)  = N(Z)
```

`S` is the live Binance price, `K` is the market's opening price, `σ` is volatility measured over the last 30 seconds of trades, and `τ` is the time left plus the oracle's reporting lag.

**Entry** — the bot trades only when all of these hold:

- It is inside the snipe window, the last ~12 seconds before expiry
- The model is confident enough (`|Z|` above a threshold)
- Expected value is positive after Polymarket's price-dependent taker fee

**Execution** — tries a maker order first (no fee), then falls back to a fill-or-kill order at a fresh price.

**Exit** — a trailing stop-loss managed by the position manager. Winning trades are held to expiry.

## Architecture

```
streams/        Binance aggTrade WebSocket, Polymarket order-book WebSocket
market/         market discovery (Gamma API) and shared market state
engine/         probability model, fee/EV filter, live strategy config
execution/      order manager (maker first, FAK fallback)
agents/         orchestrator, position manager, evaluator, strategy research
learning_agent/ reviewer → researcher → developer → validator pipeline
core/           SQLite storage, P&L tracking, risk manager
monitor/        kill switch, latency monitors, Telegram alerts
simulation/     paper trader
dashboard.py    Streamlit dashboard
```

The **orchestrator** supervises every agent, wires their queues together, and restarts any agent that fails.

## The learning pipeline

No strategy change goes live without passing a gate:

```
Proposal → Backtest (≥15 trades, ≥52% win rate)
         → Paper trading (≥20 trades and ≥2 hours)
         → All validation gates pass → promoted to live config
```

- Agents can only change `strategy_config.json`. They never edit source code.
- The engine re-reads the config every 10 seconds, so a promoted strategy takes effect without a restart.
- Every experiment is written to an audit log.

## Safety

- **Paper mode by default.** Live trading needs `--mode live`.
- **Credential check before live mode** — the bot refuses to start with missing or malformed credentials.
- **Risk manager** — fractional Kelly bet sizing and a circuit breaker on session drawdown.
- **Kill switch** — halts trading when network latency is too high.
- **Event-loop lag monitor** — detects when the bot itself is too slow to trade safely.

## Running

```bash
pip install -r requirements.txt
cp .env.example .env        # fill in your own values

python main.py              # paper trading
python main.py --status     # session stats from the database
python main.py --resolve    # score pending paper trades
streamlit run dashboard.py  # dashboard
```

## Tech stack

Python · asyncio (uvloop / winloop) · WebSockets · SQLite · SciPy · Streamlit · Polymarket CLOB V2 · Binance API · Telegram Bot API

## What I'd do differently

- Add unit tests for the probability model and the fee filter.
- The strategy research agent mutates and backtests parameters locally. It does not pull outside research, despite the name.
- Replace the local SQLite file with a proper database if more than one instance runs.

## Disclaimer

Personal project built to learn. Not financial advice.
