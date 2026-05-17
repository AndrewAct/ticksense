import structlog
from fastapi import APIRouter, Depends

from ..dependencies import get_client
from ..models.symbols import SymbolItem, SymbolsResponse
from ..trino_client import TrinoClient

log = structlog.get_logger()
router = APIRouter(prefix="/symbols", tags=["symbols"])

_SQL = """
    SELECT symbol, exchange, status, base_asset, quote_asset, updated_at
    FROM iceberg.normalized.symbol_config
    ORDER BY symbol
"""


@router.get("", response_model=SymbolsResponse)
async def list_symbols(
    client: TrinoClient = Depends(get_client),
) -> SymbolsResponse:
    rows = await client.fetch(_SQL)
    symbols = [SymbolItem.model_validate(r) for r in rows]
    return SymbolsResponse(symbols=symbols, count=len(symbols))
