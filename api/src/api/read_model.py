from __future__ import annotations

from dataclasses import dataclass, field

from .models.liquidity import LiquidityResponse, SpreadResponse
from .models.ohlcv import OHLCVResponse
from .models.pipeline import PipelineLagResponse
from .models.symbols import SymbolsResponse


@dataclass
class ReadModel:
    """Process-local read model populated by the background poller.

    All fields are replaced atomically (full dict swap or object assignment).
    CPython's GIL guarantees that a reference swap is atomic, so no lock is
    needed for the reads that happen in request handlers.

    ready is False until the first full poll cycle completes.  Endpoints
    return 503 until ready is True.
    """

    spread: dict[str, SpreadResponse] = field(default_factory=dict)
    liquidity: dict[str, LiquidityResponse] = field(default_factory=dict)
    # last 60 bars per symbol, newest-first (mirrors the original Trino ORDER BY DESC)
    ohlcv: dict[str, OHLCVResponse] = field(default_factory=dict)
    pipeline: PipelineLagResponse | None = None
    symbols: SymbolsResponse | None = None
    ready: bool = False


_model = ReadModel()


def get_read_model() -> ReadModel:
    return _model
