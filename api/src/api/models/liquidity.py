from datetime import datetime

from pydantic import BaseModel


class SpreadResponse(BaseModel):
    exchange: str
    symbol: str
    latest_ts: datetime
    best_bid_price: float
    best_ask_price: float
    spread: float
    mid_price: float
    spread_bps: float


class LiquidityResponse(BaseModel):
    exchange: str
    symbol: str
    latest_ts: datetime
    best_bid_price: float
    best_ask_price: float
    spread: float
    mid_price: float
    spread_bps: float
    best_bid_qty: float
    best_ask_qty: float
    imbalance: float
    market_signal: str
    staleness_seconds: int
    freshness_status: str
