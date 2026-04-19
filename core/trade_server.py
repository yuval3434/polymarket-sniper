"""
Trade Server — HTTP API for cross-machine learning sync.
Each bot instance runs this on TRADE_SERVER_PORT (default 8765).
Peers fetch /trades to pull resolved trade history.
"""
from __future__ import annotations

import asyncio
import json
import logging

import aiohttp.web as web

from config import TRADE_SERVER_PORT
from core.database import fetch_resolved_trades_with_context

logger = logging.getLogger(__name__)


async def _handle_trades(request: web.Request) -> web.Response:
    try:
        rows = fetch_resolved_trades_with_context()
        payload = [
            {
                "timestamp":       r.get("timestamp", ""),
                "market_id":       r.get("market_id", ""),
                "token_side":      r.get("token_side", ""),
                "entry_price":     float(r.get("entry_price") or 0),
                "entry_size_usdc": float(r.get("entry_size_usdc") or 0),
                "size_shares":     float(r.get("size_shares") or 0),
                "exit_price":      float(r.get("exit_price") or 0),
                "exit_type":       r.get("exit_type", ""),
                "pnl_usd":         float(r.get("pnl_usd") or 0),
                "pnl_pct":         float(r.get("pnl_pct") or 0),
                "win":             int(r.get("win") or 0),
                "p_true":          float(r.get("p_true") or 0.5),
                "z_score":         float(r.get("z_score") or 0),
                "dry_run":         int(r.get("dry_run") or 1),
            }
            for r in rows
        ]
        return web.Response(
            text=json.dumps(payload),
            content_type="application/json",
        )
    except Exception as exc:
        logger.warning("Trade server /trades error: %s", exc)
        return web.Response(status=500, text=str(exc))


async def _handle_health(request: web.Request) -> web.Response:
    return web.Response(text='{"status":"ok"}', content_type="application/json")


async def trade_server_loop() -> None:
    """Serve local resolved trades over HTTP so peer instances can learn from them."""
    app = web.Application()
    app.router.add_get("/trades", _handle_trades)
    app.router.add_get("/health", _handle_health)

    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "0.0.0.0", TRADE_SERVER_PORT, reuse_address=True)
        await site.start()
    except OSError as exc:
        logger.warning(
            "Trade server: port %d already in use — another instance is running. "
            "Sleeping 60s before retry. (%s)", TRADE_SERVER_PORT, exc,
        )
        await runner.cleanup()
        await asyncio.sleep(60)
        raise  # let _run_agent restart with fresh coroutine

    logger.info("Trade server listening on 0.0.0.0:%d  (GET /trades for peers)", TRADE_SERVER_PORT)
    while True:
        await asyncio.sleep(3600)
