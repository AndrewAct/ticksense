from datetime import datetime

from pydantic import BaseModel


class SymbolItem(BaseModel):
    symbol: str
    exchange: str
    status: str
    base_asset: str
    quote_asset: str
    updated_at: datetime | None = None


class SymbolsResponse(BaseModel):
    symbols: list[SymbolItem]
    count: int
