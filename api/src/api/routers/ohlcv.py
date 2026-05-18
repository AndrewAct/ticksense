from datetime import datetime
from threading import Lock
from typing import Annotated

import structlog
from cachetools import TTLCache
from fastapi import APIRouter, Depends, HTTPException, Query

from ..dependencies import get_client
from ..models.ohlcv import OHLCVBar, OHLCVResponse
from ..read_model import get_read_model
from ..trino_client import TrinoClient

log = structlog.get_logger()
router = APIRouter(prefix="/ohlcv", tags=["ohlcv"])

# The poller pre-loads this many bars per symbol (newest-first).
# Requests within this limit are served from the in-memory model.
_PRELOADED_BARS = 60

# Cold-path cache: historical range queries are rare but may be repeated
# (e.g., a dashboard refreshing the same date range every minute).
# TTL=60s aligns with the bar interval; maxsize covers 500 distinct queries.
_cold_cache: TTLCache[tuple, OHLCVResponse] = TTLCache(maxsize=500, ttl=60)
_cold_lock = Lock()

_COLS = """
    exchange, symbol, window_start, window_end,
    open_price, high_price, low_price, close_price,
    volume, vwap, tick_count, first_ts, last_ts
"""


@router.get("/{symbol}", response_model=OHLCVResponse)
async def get_ohlcv(
    symbol: str,
    limit: Annotated[int, Query(ge=1, le=1440)] = 60,
    from_ts: Annotated[datetime | None, Query()] = None,
    to_ts: Annotated[datetime | None, Query()] = None,
    client: TrinoClient = Depends(get_client),
) -> OHLCVResponse:
    sym = symbol.upper()
    model = get_read_model()

    # ── Hot path ───────────────────────────────────────────────────────────────
    # No time-range filter and limit fits what the poller pre-loaded:
    # serve from the in-memory model — sub-millisecond, no Trino call.
    if from_ts is None and to_ts is None and limit <= _PRELOADED_BARS:
        if not model.ready:
            raise HTTPException(status_code=503, detail="Service warming up")
        cached = model.ohlcv.get(sym)
        if cached is None:
            raise HTTPException(status_code=404, detail=f"No OHLCV data for symbol {sym}")
        bars = cached.bars[:limit]
        log.debug("ohlcv_model_hit", symbol=sym, limit=limit)
        return OHLCVResponse(symbol=sym, exchange=cached.exchange, bars=bars, count=len(bars))

    # ── Cold path ──────────────────────────────────────────────────────────────
    # Historical range query (from_ts / to_ts set) or limit > 60: hit Trino.
    # These are analytical / backfill requests; higher latency is acceptable,
    # but repeated identical queries are cached for 60 s.
    cache_key = (sym, limit, from_ts, to_ts)
    with _cold_lock:
        cold_cached: OHLCVResponse | None = _cold_cache.get(cache_key)
    if cold_cached is not None:
        log.debug("ohlcv_cold_cache_hit", symbol=sym)
        return cold_cached

    sql_parts = [f"SELECT {_COLS} FROM iceberg.marts.mart_ohlcv WHERE LOWER(symbol) = LOWER(?)"]
    params: list[object] = [sym]
    if from_ts:
        sql_parts.append("AND window_start >= ?")
        params.append(from_ts)
    if to_ts:
        sql_parts.append("AND window_end <= ?")
        params.append(to_ts)
    sql_parts.extend(["ORDER BY window_start DESC", "LIMIT ?"])
    params.append(limit)

    rows = await client.fetch("\n".join(sql_parts), params)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No OHLCV data for symbol {sym}")
    bars_list = [OHLCVBar.model_validate(r) for r in rows]
    log.info("ohlcv_trino_served", symbol=sym, bars=len(bars_list))
    result = OHLCVResponse(
        symbol=sym, exchange=str(rows[0]["exchange"]), bars=bars_list, count=len(bars_list)
    )
    with _cold_lock:
        _cold_cache[cache_key] = result
    return result
