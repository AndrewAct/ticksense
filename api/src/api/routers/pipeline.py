from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, Depends

from ..dependencies import get_client
from ..models.pipeline import PipelineLagItem, PipelineLagResponse
from ..trino_client import TrinoClient

log = structlog.get_logger()
router = APIRouter(prefix="/pipeline", tags=["pipeline"])

_SQL = """
    SELECT exchange, symbol, latest_event_ts, latest_ingest_ts,
           staleness_seconds, freshness_status, health_score
    FROM iceberg.marts.mart_exchange_health
    ORDER BY symbol
"""


@router.get("/lag", response_model=PipelineLagResponse)
async def get_pipeline_lag(
    client: TrinoClient = Depends(get_client),
) -> PipelineLagResponse:
    rows = await client.fetch(_SQL)
    items = [PipelineLagItem.model_validate(r) for r in rows]
    healthy = sum(1 for i in items if i.freshness_status == "FRESH")
    log.info("pipeline_lag_served", total=len(items), healthy=healthy)
    return PipelineLagResponse(
        items=items,
        checked_at=datetime.now(UTC),
        healthy_count=healthy,
        total_count=len(items),
    )
