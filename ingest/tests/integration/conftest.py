"""Auto-skip integration tests when Docker is not available."""

import pytest


def _docker_available() -> bool:
    try:
        import docker  # type: ignore[import-untyped]

        docker.from_env().ping()
        return True
    except Exception:
        return False


if not _docker_available():
    pytest.skip("Docker not available — skipping integration tests", allow_module_level=True)
