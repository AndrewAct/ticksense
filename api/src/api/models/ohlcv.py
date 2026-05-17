from datetime import datetime

from pydantic import BaseModel


class OHLCVBar(BaseModel):
    exchange: str
    symbol: str
    window_start: datetime
    window_end: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    vwap: float
    tick_count: int
    first_ts: datetime
    last_ts: datetime


class OHLCVResponse(BaseModel):
    symbol: str
    exchange: str
    interval: str = "1m"
    bars: list[OHLCVBar]
    count: int
