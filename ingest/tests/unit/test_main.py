"""Unit tests for the ingest entry point."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingest.main import _run


@pytest.mark.asyncio
async def test_run_starts_producer_and_clients() -> None:
    mock_prod = AsyncMock()
    mock_prod.start = AsyncMock()
    mock_prod.stop = AsyncMock()

    mock_client = MagicMock()
    mock_client.run = AsyncMock()

    with (
        patch("ingest.main.KafkaOrderBookProducer", return_value=mock_prod),
        patch("ingest.main.BinanceOrderBookClient", return_value=mock_client),
        patch("ingest.main.settings") as mock_settings,
    ):
        mock_settings.ingest_symbols = ["btcusdt"]
        mock_settings.log_level = "INFO"
        await _run()

    mock_prod.start.assert_called_once()
    mock_client.run.assert_called_once()
    mock_prod.stop.assert_called_once()


@pytest.mark.asyncio
async def test_run_calls_stop_on_exception() -> None:
    """producer.stop() is always called, even if a client crashes."""
    mock_prod = AsyncMock()
    mock_prod.start = AsyncMock()
    mock_prod.stop = AsyncMock()

    mock_client = MagicMock()
    mock_client.run = AsyncMock(side_effect=RuntimeError("client crashed"))

    with (
        patch("ingest.main.KafkaOrderBookProducer", return_value=mock_prod),
        patch("ingest.main.BinanceOrderBookClient", return_value=mock_client),
        patch("ingest.main.settings") as mock_settings,
        pytest.raises(RuntimeError, match="client crashed"),
    ):
        mock_settings.ingest_symbols = ["btcusdt"]
        mock_settings.log_level = "INFO"
        await _run()

    mock_prod.stop.assert_called_once()


@pytest.mark.asyncio
async def test_run_creates_client_per_symbol() -> None:
    mock_prod = AsyncMock()
    mock_prod.start = AsyncMock()
    mock_prod.stop = AsyncMock()

    clients_created: list[str] = []

    def make_client(symbol: str, **_: object) -> MagicMock:
        clients_created.append(symbol)
        c = MagicMock()
        c.run = AsyncMock()
        return c

    with (
        patch("ingest.main.KafkaOrderBookProducer", return_value=mock_prod),
        patch("ingest.main.BinanceOrderBookClient", side_effect=make_client),
        patch("ingest.main.settings") as mock_settings,
    ):
        mock_settings.ingest_symbols = ["btcusdt", "ethusdt", "solusdt"]
        mock_settings.log_level = "INFO"
        await _run()

    assert len(clients_created) == 3
    assert "btcusdt" in clients_created
    assert "ethusdt" in clients_created
