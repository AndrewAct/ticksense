from .config import settings
from .trino_client import TrinoClient


def get_client() -> TrinoClient:
    return TrinoClient(
        host=settings.trino_host,
        port=settings.trino_port,
        user=settings.trino_user,
        catalog=settings.trino_catalog,
    )
