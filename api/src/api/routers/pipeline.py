import structlog
from fastapi import APIRouter, HTTPException

from ..models.pipeline import PipelineLagResponse
from ..read_model import get_read_model

log = structlog.get_logger()
router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@router.get("/lag", response_model=PipelineLagResponse)
async def get_pipeline_lag() -> PipelineLagResponse:
    model = get_read_model()
    if not model.ready:
        raise HTTPException(status_code=503, detail="Service warming up")
    if model.pipeline is None:
        raise HTTPException(status_code=503, detail="Service warming up")
    log.info("pipeline_lag_served", total=model.pipeline.total_count)
    return model.pipeline
