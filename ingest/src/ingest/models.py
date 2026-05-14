from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class BinanceDepthEvent(BaseModel):
    """Raw Binance diff depth WebSocket message (depthUpdate stream)."""

    model_config = ConfigDict(populate_by_name=True)

    event_type: str = Field(alias="e")
    event_time_ms: int = Field(alias="E")
    symbol: str = Field(alias="s")
    first_update_id: int = Field(alias="U")
    last_update_id: int = Field(alias="u")
    bids: list[tuple[str, str]] = Field(alias="b")
    asks: list[tuple[str, str]] = Field(alias="a")


class BinanceDepthSnapshot(BaseModel):
    """REST /api/v3/depth response."""

    model_config = ConfigDict(populate_by_name=True)

    last_update_id: int = Field(alias="lastUpdateId")
    bids: list[tuple[str, str]]
    asks: list[tuple[str, str]]


class OrderBookEvent(BaseModel):
    """Normalized L2 depth diff event — written to market.raw.orderbook (bronze)."""

    event_id: str  # dedup key: binance#{symbol}#{last_update_id}
    exchange: str = "binance"
    symbol: str
    exchange_event_ts: datetime
    ingest_ts: datetime
    first_update_id: int
    last_update_id: int
    bids: list[tuple[str, str]]
    asks: list[tuple[str, str]]
    # Populated after produce
    kafka_topic: str | None = None
    kafka_partition: int | None = None
    kafka_offset: int | None = None


class DLQEvent(BaseModel):
    """Dead letter queue envelope."""

    original_key: str | None
    original_payload: str
    error_type: str
    error_message: str
    ingest_ts: datetime
