"""Read SQL files from spark/jobs/sql/ and substitute named parameters."""

from __future__ import annotations

from pathlib import Path

_SQL_DIR = Path(__file__).parent.parent / "sql"


def load_sql(filename: str, **params: str) -> str:
    """Return the contents of sql/<filename>, with {params} substituted."""
    text = (_SQL_DIR / filename).read_text()
    return text.format(**params) if params else text
