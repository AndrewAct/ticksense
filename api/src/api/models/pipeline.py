from datetime import datetime

from pydantic import BaseModel


class PipelineLagItem(BaseModel):
    exchange: str
    symbol: str
    latest_event_ts: datetime
    latest_ingest_ts: datetime
    staleness_seconds: int
    freshness_status: str
    health_score: float


class PipelineLagResponse(BaseModel):
    items: list[PipelineLagItem]
    checked_at: datetime
    healthy_count: int
    total_count: int
