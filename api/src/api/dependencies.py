from functools import lru_cache

from .config import settings
from .trino_client import TrinoClient


@lru_cache(maxsize=1)
def get_client() -> TrinoClient:
    return TrinoClient(
        host=settings.trino_host,
        port=settings.trino_port,
        user=settings.trino_user,
        catalog=settings.trino_catalog,
    )
