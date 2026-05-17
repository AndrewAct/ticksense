import structlog
from fastapi import APIRouter, Depends, HTTPException

from ..dependencies import get_client
from ..models.liquidity import LiquidityResponse, SpreadResponse
from ..trino_client import TrinoClient

log = structlog.get_logger()
router = APIRouter(tags=["liquidity"])

_LIQUIDITY_COLS = """
    exchange, symbol, latest_ts,
    best_bid_price, best_ask_price, spread, mid_price, spread_bps,
    best_bid_qty, best_ask_qty, imbalance, market_signal,
    staleness_seconds, freshness_status
"""

_SPREAD_COLS = """
    exchange, symbol, latest_ts,
    best_bid_price, best_ask_price, spread, mid_price, spread_bps
"""


@router.get("/spread/{symbol}", response_model=SpreadResponse)
async def get_spread(
    symbol: str,
    client: TrinoClient = Depends(get_client),
) -> SpreadResponse:
    sym = symbol.upper()
    sql = f"SELECT {_SPREAD_COLS} FROM iceberg.marts.mart_liquidity WHERE LOWER(symbol) = LOWER(?)"
    rows = await client.fetch(sql, [sym])
    if not rows:
        raise HTTPException(status_code=404, detail=f"No spread data for symbol {sym}")
    return SpreadResponse.model_validate(rows[0])


@router.get("/liquidity/{symbol}", response_model=LiquidityResponse)
async def get_liquidity(
    symbol: str,
    client: TrinoClient = Depends(get_client),
) -> LiquidityResponse:
    sym = symbol.upper()
    sql = (
        f"SELECT {_LIQUIDITY_COLS} FROM iceberg.marts.mart_liquidity WHERE LOWER(symbol) = LOWER(?)"
    )
    rows = await client.fetch(sql, [sym])
    if not rows:
        raise HTTPException(status_code=404, detail=f"No liquidity data for symbol {sym}")
    log.info("liquidity_served", symbol=sym, signal=rows[0].get("market_signal"))
    return LiquidityResponse.model_validate(rows[0])
