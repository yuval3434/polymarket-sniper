"""
Polymarket BTC Sniper — Live Dashboard
=======================================
Run independently from the main bot:
    streamlit run dashboard.py

Reads trades_history.csv (written by core/tracker.py after each close).
Refreshes automatically every 15 seconds when auto-refresh is enabled.
"""

import os
import time

import pandas as pd
import streamlit as st

CSV_PATH = os.path.join(os.path.dirname(__file__), "trades_history.csv")

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Polymarket Sniper Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .metric-label  { font-size: 0.85rem; color: #aaa; }
    .metric-value  { font-size: 2.4rem; font-weight: 700; }
    .win-tag       { color: #00c77a; font-weight: 700; }
    .loss-tag      { color: #ff4c4c; font-weight: 700; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------

st.sidebar.title("⚙️ Controls")
auto_refresh  = st.sidebar.checkbox("Auto-refresh (15 s)", value=True)
mode_filter   = st.sidebar.selectbox("Mode", ["All", "Paper Only", "Live Only"])
n_recent      = st.sidebar.slider("Recent trades to show", 10, 50, 20)

if st.sidebar.button("🔄 Refresh now"):
    st.cache_data.clear()
    st.rerun()

# ---------------------------------------------------------------------------
# Data loader (TTL-cached so re-reads CSV at most every 10s)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=10)
def _load_csv() -> pd.DataFrame:
    if not os.path.exists(CSV_PATH):
        return pd.DataFrame()
    df = pd.read_csv(CSV_PATH)
    if df.empty:
        return df
    df["entry_timestamp"] = pd.to_datetime(df["entry_timestamp"], utc=True, errors="coerce")
    df["exit_timestamp"]  = pd.to_datetime(df["exit_timestamp"],  utc=True, errors="coerce")
    df["pnl_usd"]  = pd.to_numeric(df["pnl_usd"],  errors="coerce").fillna(0.0)
    df["pnl_pct"]  = pd.to_numeric(df["pnl_pct"],  errors="coerce").fillna(0.0)
    df["dry_run"]  = df["dry_run"].astype(int)
    return df

df_raw = _load_csv()

# ---------------------------------------------------------------------------
# No-data guard
# ---------------------------------------------------------------------------

if df_raw.empty:
    st.title("📈 Polymarket Sniper — Dashboard")
    st.warning(
        "No trades recorded yet.  \n"
        "Start the bot (`py main.py`) and trades will appear here automatically."
    )
    if auto_refresh:
        time.sleep(15)
        st.rerun()
    st.stop()

# ---------------------------------------------------------------------------
# Apply mode filter
# ---------------------------------------------------------------------------

df = df_raw.copy()
if mode_filter == "Paper Only":
    df = df[df["dry_run"] == 1]
elif mode_filter == "Live Only":
    df = df[df["dry_run"] == 0]

if df.empty:
    st.warning(f"No trades match filter: {mode_filter}")
    if auto_refresh:
        time.sleep(15)
        st.rerun()
    st.stop()

df = df.sort_values("exit_timestamp").reset_index(drop=True)

# ---------------------------------------------------------------------------
# Core metrics
# ---------------------------------------------------------------------------

total_pnl   = df["pnl_usd"].sum()
total_trades = len(df)
wins         = (df["outcome"] == "WIN").sum()
losses       = total_trades - wins
win_rate     = wins / total_trades if total_trades else 0.0
avg_pnl      = df["pnl_usd"].mean()
best_trade   = df["pnl_usd"].max()
worst_trade  = df["pnl_usd"].min()

# Running drawdown
df["equity"]  = df["pnl_usd"].cumsum()
peak          = df["equity"].cummax()
drawdown      = (peak - df["equity"]).max()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("📈 Polymarket BTC Sniper — Live Dashboard")
st.caption(f"Data from: `{CSV_PATH}`  ·  {total_trades} trades  ·  Mode: {mode_filter}")

# ---------------------------------------------------------------------------
# Top KPI row (giant numbers)
# ---------------------------------------------------------------------------

pnl_color  = "#00c77a" if total_pnl >= 0 else "#ff4c4c"
pnl_sign   = "+" if total_pnl >= 0 else ""
avg_sign   = "+" if avg_pnl >= 0 else ""

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

kpi1.metric(
    label="💰 Net P&L (USDC)",
    value=f"{pnl_sign}${total_pnl:.4f}",
    delta=f"{pnl_sign}{total_pnl:.4f}",
)
kpi2.metric(
    label="🎯 Win Rate",
    value=f"{win_rate:.1%}",
    delta=f"{wins}W / {losses}L",
)
kpi3.metric(
    label="📊 Total Trades",
    value=str(total_trades),
)
kpi4.metric(
    label="📈 Avg P&L / Trade",
    value=f"{avg_sign}${avg_pnl:.4f}",
)
kpi5.metric(
    label="📉 Max Drawdown",
    value=f"-${drawdown:.4f}",
)

st.divider()

# ---------------------------------------------------------------------------
# Equity curve
# ---------------------------------------------------------------------------

st.subheader("📉 Equity Curve")

equity_df = df[["exit_timestamp", "equity"]].copy()
equity_df = equity_df.set_index("exit_timestamp")

# Add a zero starting point for visual clarity
zero_row = pd.DataFrame({"equity": [0.0]}, index=[equity_df.index[0] - pd.Timedelta(seconds=1)])
equity_plot = pd.concat([zero_row, equity_df])

st.line_chart(equity_plot, color="#00c77a" if total_pnl >= 0 else "#ff4c4c")

st.divider()

# ---------------------------------------------------------------------------
# Recent trades table
# ---------------------------------------------------------------------------

st.subheader(f"📋 Recent {n_recent} Trades")

recent = df.tail(n_recent).sort_values("exit_timestamp", ascending=False).copy()
recent["exit_timestamp_str"] = recent["exit_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
recent["pnl_usd_fmt"]  = recent["pnl_usd"].apply(lambda x: f"+${x:.4f}" if x >= 0 else f"-${abs(x):.4f}")
recent["pnl_pct_fmt"]  = recent["pnl_pct"].apply(lambda x: f"+{x:.1f}%" if x >= 0 else f"{x:.1f}%")

display = recent[[
    "exit_timestamp_str", "token_side", "entry_price", "exit_price",
    "size_usdc", "pnl_usd_fmt", "pnl_pct_fmt", "outcome", "exit_reason", "strategy_name",
]].rename(columns={
    "exit_timestamp_str": "Closed At",
    "token_side":         "Side",
    "entry_price":        "Entry $",
    "exit_price":         "Exit $",
    "size_usdc":          "Size (USDC)",
    "pnl_usd_fmt":        "P&L (USDC)",
    "pnl_pct_fmt":        "P&L %",
    "outcome":            "Result",
    "exit_reason":        "Exit Type",
    "strategy_name":      "Strategy",
})


def _color_result(val: str) -> str:
    if val == "WIN":
        return "background-color: #0d2b1a; color: #00c77a; font-weight: bold"
    return "background-color: #2b0d0d; color: #ff4c4c; font-weight: bold"


def _color_pnl(val: str) -> str:
    if val.startswith("+"):
        return "color: #00c77a"
    return "color: #ff4c4c"


styled = (
    display.style
    .applymap(_color_result, subset=["Result"])
    .applymap(_color_pnl,    subset=["P&L (USDC)", "P&L %"])
)

st.dataframe(styled, use_container_width=True, hide_index=True)

st.divider()

# ---------------------------------------------------------------------------
# Strategy performance
# ---------------------------------------------------------------------------

st.subheader("🧠 Strategy Performance")

strat = (
    df.groupby("strategy_name")
    .agg(
        Trades   =("trade_id",  "count"),
        Wins     =("outcome",   lambda x: (x == "WIN").sum()),
        Win_Rate =("outcome",   lambda x: round((x == "WIN").mean() * 100, 1)),
        Total_PnL=("pnl_usd",   "sum"),
        Avg_PnL  =("pnl_usd",   "mean"),
    )
    .reset_index()
    .rename(columns={"strategy_name": "Strategy"})
    .sort_values("Total_PnL", ascending=False)
)
strat["Win Rate %"] = strat["Win_Rate"].astype(str) + "%"
strat["Total_PnL"]  = strat["Total_PnL"].round(4)
strat["Avg_PnL"]    = strat["Avg_PnL"].round(4)

col_chart, col_table = st.columns([2, 1])

with col_chart:
    chart_data = strat.set_index("Strategy")[["Total_PnL"]].rename(columns={"Total_PnL": "Net P&L (USDC)"})
    st.bar_chart(chart_data)

with col_table:
    st.dataframe(
        strat[["Strategy", "Trades", "Win Rate %", "Total_PnL", "Avg_PnL"]].rename(
            columns={"Total_PnL": "Net P&L", "Avg_PnL": "Avg/Trade"}
        ),
        use_container_width=True,
        hide_index=True,
    )

st.divider()

# ---------------------------------------------------------------------------
# Exit type breakdown
# ---------------------------------------------------------------------------

st.subheader("🚪 Exit Type Breakdown")

exit_stats = (
    df.groupby("exit_reason")
    .agg(
        Count   =("trade_id", "count"),
        Win_Rate=("outcome",  lambda x: round((x == "WIN").mean() * 100, 1)),
        Net_PnL =("pnl_usd",  "sum"),
    )
    .reset_index()
    .rename(columns={"exit_reason": "Exit Type", "Win_Rate": "Win Rate %", "Net_PnL": "Net P&L"})
)

st.dataframe(exit_stats, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Footer / auto-refresh
# ---------------------------------------------------------------------------

st.caption(f"Last updated: {pd.Timestamp.now().strftime('%H:%M:%S')}  ·  Auto-refresh: {'ON' if auto_refresh else 'OFF'}")

if auto_refresh:
    time.sleep(15)
    st.rerun()
