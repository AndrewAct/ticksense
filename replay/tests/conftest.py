"""Shared pytest fixtures for replay tests."""

from __future__ import annotations

import pytest

from replay.config import Settings


@pytest.fixture()
def settings() -> Settings:
    """Default Settings with local dev defaults (no running services needed)."""
    return Settings()
