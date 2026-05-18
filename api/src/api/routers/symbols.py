from fastapi import APIRouter, HTTPException

from ..models.symbols import SymbolsResponse
from ..read_model import get_read_model

router = APIRouter(prefix="/symbols", tags=["symbols"])


@router.get("", response_model=SymbolsResponse)
async def list_symbols() -> SymbolsResponse:
    model = get_read_model()
    if not model.ready or model.symbols is None:
        raise HTTPException(status_code=503, detail="Service warming up")
    return model.symbols
