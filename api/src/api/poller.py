"""Background poller — the single writer of the in-process ReadModel.

Refresh cadence
---------------
Liquidity + spread  30 s   (order book data, changes every tick)
Pipeline health     30 s   (staleness / freshness metrics)
OHLCV last-60       60 s   (aligned with Flink's 1-minute bar interval)
Symbol config        5 min  (CDC-driven, changes very rarely)

On startup all four are refreshed concurrently so the model is ready before
the first request arrives.  After that, only the due refreshes run each tick.

Prometheus gauges are updated alongside the read model so Grafana still works.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Coroutine
from datetime import UTC, datetime
from typing import Any

import structlog

from .market_metrics import HEALTH_SCORE, IMBALANCE, MID_PRICE, SPREAD_BPS, STALENESS
from .models.liquidity import LiquidityResponse, SpreadResponse
from .models.ohlcv import OHLCVBar, OHLCVResponse
from .models.pipeline import PipelineLagItem, PipelineLagResponse
from .models.symbols import SymbolItem, SymbolsResponse
from .read_model import _model
from .trino_client import TrinoClient

log = structlog.get_logger()

_LIQUIDITY_INTERVAL = 30
_OHLCV_INTERVAL = 60
_SYMBOLS_INTERVAL = 300

_LIQUIDITY_SQL = """
    SELECT exchange, symbol, latest_ts,
           best_bid_price, best_ask_price, spread, mid_price, spread_bps,
           best_bid_qty, best_ask_qty, COALESCE(imbalance, 0.0) AS imbalance,
           market_signal, staleness_seconds, freshness_status
    FROM iceberg.marts.mart_liquidity
"""

# One query for all symbols: window function picks the 60 most-recent bars per
# symbol, bounded to the last 2 hours so the table scan stays cheap.
_OHLCV_SQL = """
    SELECT exchange, symbol, window_start, window_end,
           open_price, high_price, low_price, close_price,
           volume, vwap, tick_count, first_ts, last_ts
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY window_start DESC) AS rn
        FROM iceberg.marts.mart_ohlcv
        WHERE window_start >= NOW() - INTERVAL '2' HOUR
    ) t
    WHERE t.rn <= 60
    ORDER BY symbol, window_start DESC
"""

_PIPELINE_SQL = """
    SELECT exchange, symbol, latest_event_ts, latest_ingest_ts,
           staleness_seconds, freshness_status, health_score
    FROM iceberg.marts.mart_exchange_health
    ORDER BY symbol
"""

_SYMBOLS_SQL = """
    SELECT symbol, exchange, status, base_asset, quote_asset, updated_at
    FROM iceberg.normalized.symbol_config
    ORDER BY symbol
"""


async def _poll_liquidity(client: TrinoClient) -> None:
    try:
        rows = await client.fetch(_LIQUIDITY_SQL)
        new_spread: dict[str, SpreadResponse] = {}
        new_liquidity: dict[str, LiquidityResponse] = {}
        for r in rows:
            sym = str(r["symbol"]).upper()
            lbl = {"symbol": str(r["symbol"]).lower(), "exchange": str(r["exchange"])}
            new_spread[sym] = SpreadResponse.model_validate(r)
            new_liquidity[sym] = LiquidityResponse.model_validate(r)
            MID_PRICE.labels(**lbl).set(r["mid_price"])
            SPREAD_BPS.labels(**lbl).set(r["spread_bps"])
            IMBALANCE.labels(**lbl).set(r["imbalance"])
            STALENESS.labels(**lbl).set(r["staleness_seconds"])
        # Atomic swap: request handlers either see the old dict or the new one.
        _model.spread = new_spread
        _model.liquidity = new_liquidity
        log.info("poll_liquidity_ok", symbols=len(rows))
    except Exception as exc:
        log.warning("poll_liquidity_failed", error=str(exc))


async def _poll_ohlcv(client: TrinoClient) -> None:
    try:
        rows = await client.fetch(_OHLCV_SQL)
        by_symbol: dict[str, list[OHLCVBar]] = {}
        exchange_by_symbol: dict[str, str] = {}
        for r in rows:
            sym = str(r["symbol"]).upper()
            by_symbol.setdefault(sym, []).append(OHLCVBar.model_validate(r))
            exchange_by_symbol[sym] = str(r["exchange"])
        _model.ohlcv = {
            sym: OHLCVResponse(
                symbol=sym,
                exchange=exchange_by_symbol[sym],
                bars=bars,
                count=len(bars),
            )
            for sym, bars in by_symbol.items()
        }
        log.info("poll_ohlcv_ok", symbols=len(by_symbol))
    except Exception as exc:
        log.warning("poll_ohlcv_failed", error=str(exc))


async def _poll_pipeline(client: TrinoClient) -> None:
    try:
        rows = await client.fetch(_PIPELINE_SQL)
        items = [PipelineLagItem.model_validate(r) for r in rows]
        for r in rows:
            HEALTH_SCORE.labels(symbol=str(r["symbol"]), exchange=str(r["exchange"])).set(
                r["health_score"]
            )
        healthy = sum(1 for i in items if i.freshness_status == "FRESH")
        _model.pipeline = PipelineLagResponse(
            items=items,
            checked_at=datetime.now(UTC),
            healthy_count=healthy,
            total_count=len(items),
        )
        log.info("poll_pipeline_ok", total=len(items), healthy=healthy)
    except Exception as exc:
        log.warning("poll_pipeline_failed", error=str(exc))


async def _poll_symbols(client: TrinoClient) -> None:
    try:
        rows = await client.fetch(_SYMBOLS_SQL)
        symbols = [SymbolItem.model_validate(r) for r in rows]
        _model.symbols = SymbolsResponse(symbols=symbols, count=len(symbols))
        log.info("poll_symbols_ok", count=len(symbols))
    except Exception as exc:
        log.warning("poll_symbols_failed", error=str(exc))


async def run_poller(client: TrinoClient) -> None:
    # Initial full refresh so the model is warm before the first request.
    await asyncio.gather(
        _poll_liquidity(client),
        _poll_pipeline(client),
        _poll_symbols(client),
        _poll_ohlcv(client),
        return_exceptions=True,
    )
    _model.ready = True

    last_ohlcv = last_symbols = time.monotonic()
    while True:
        await asyncio.sleep(30)
        t = time.monotonic()
        tasks: list[Coroutine[Any, Any, None]] = [_poll_liquidity(client), _poll_pipeline(client)]
        if t - last_ohlcv >= _OHLCV_INTERVAL:
            tasks.append(_poll_ohlcv(client))
            last_ohlcv = t
        if t - last_symbols >= _SYMBOLS_INTERVAL:
            tasks.append(_poll_symbols(client))
            last_symbols = t
        await asyncio.gather(*tasks, return_exceptions=True)
