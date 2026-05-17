from typing import Any

import anyio
import structlog
import trino

log = structlog.get_logger()


class TrinoClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8082,
        user: str = "api",
        catalog: str = "iceberg",
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._catalog = catalog

    def _connect(self) -> Any:
        return trino.dbapi.connect(  # type: ignore[no-untyped-call]
            host=self._host,
            port=self._port,
            user=self._user,
            catalog=self._catalog,
            http_scheme="http",
        )

    async def fetch(self, sql: str, params: list[object] | None = None) -> list[dict[str, Any]]:
        def _run() -> list[dict[str, Any]]:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute(sql, params or [])
                desc = cur.description
                if desc is None:
                    return []
                cols = [col[0] for col in desc]
                return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]
            finally:
                conn.close()

        return await anyio.to_thread.run_sync(_run)

    async def ping(self) -> bool:
        try:
            rows = await self.fetch("SELECT 1")
            return len(rows) == 1
        except Exception:
            return False
