from datetime import datetime
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from ..dependencies import get_client
from ..models.ohlcv import OHLCVBar, OHLCVResponse
from ..trino_client import TrinoClient

log = structlog.get_logger()
router = APIRouter(prefix="/ohlcv", tags=["ohlcv"])

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

    bars = [OHLCVBar.model_validate(r) for r in rows]
    log.info("ohlcv_served", symbol=sym, bars=len(bars))
    return OHLCVResponse(symbol=sym, exchange=str(rows[0]["exchange"]), bars=bars, count=len(bars))
