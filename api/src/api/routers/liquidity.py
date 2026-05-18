import structlog
from fastapi import APIRouter, HTTPException

from ..models.liquidity import LiquidityResponse, SpreadResponse
from ..read_model import get_read_model

log = structlog.get_logger()
router = APIRouter(tags=["liquidity"])


def _check_ready() -> None:
    if not get_read_model().ready:
        raise HTTPException(status_code=503, detail="Service warming up")


@router.get("/spread/{symbol}", response_model=SpreadResponse)
async def get_spread(symbol: str) -> SpreadResponse:
    _check_ready()
    sym = symbol.upper()
    result = get_read_model().spread.get(sym)
    if result is None:
        raise HTTPException(status_code=404, detail=f"No spread data for symbol {sym}")
    return result


@router.get("/liquidity/{symbol}", response_model=LiquidityResponse)
async def get_liquidity(symbol: str) -> LiquidityResponse:
    _check_ready()
    sym = symbol.upper()
    result = get_read_model().liquidity.get(sym)
    if result is None:
        raise HTTPException(status_code=404, detail=f"No liquidity data for symbol {sym}")
    log.info("liquidity_served", symbol=sym, signal=result.market_signal)
    return result
