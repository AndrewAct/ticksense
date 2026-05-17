"""Unit tests for lib/kafka_schema.py — no PyFlink dependency."""

from unittest.mock import MagicMock

from lib.kafka_schema import OFFSET_IDX, PARTITION_IDX, PAYLOAD_IDX, parse_kafka_record


def _make_record(
    value_bytes: bytes | None,
    partition: int = 0,
    offset: int = 0,
) -> MagicMock:
    record = MagicMock()
    record.value.return_value = value_bytes
    record.partition.return_value = partition
    record.offset.return_value = offset
    return record


class TestParseKafkaRecord:
    def test_returns_payload_partition_offset(self) -> None:
        record = _make_record(b'{"symbol": "BTCUSDT"}', partition=2, offset=12345)
        result = parse_kafka_record(record)
        assert result is not None
        assert result[PAYLOAD_IDX] == '{"symbol": "BTCUSDT"}'
        assert result[PARTITION_IDX] == 2
        assert result[OFFSET_IDX] == 12345

    def test_tombstone_returns_none(self) -> None:
        record = _make_record(None, partition=0, offset=99)
        assert parse_kafka_record(record) is None

    def test_partition_zero_and_offset_zero_are_valid(self) -> None:
        record = _make_record(b"{}", partition=0, offset=0)
        result = parse_kafka_record(record)
        assert result is not None
        assert result[PARTITION_IDX] == 0
        assert result[OFFSET_IDX] == 0

    def test_utf8_payload_decoded_correctly(self) -> None:
        payload = '{"exchange": "binance", "symbol": "ETHUSDT"}'
        record = _make_record(payload.encode("utf-8"), partition=1, offset=500)
        result = parse_kafka_record(record)
        assert result is not None
        assert result[PAYLOAD_IDX] == payload

    def test_large_offset_handled(self) -> None:
        large_offset = 2**40
        record = _make_record(b"{}", partition=0, offset=large_offset)
        result = parse_kafka_record(record)
        assert result is not None
        assert result[OFFSET_IDX] == large_offset

    def test_high_partition_number(self) -> None:
        record = _make_record(b"{}", partition=47, offset=1)
        result = parse_kafka_record(record)
        assert result is not None
        assert result[PARTITION_IDX] == 47

    def test_index_constants_are_distinct(self) -> None:
        assert len({PAYLOAD_IDX, PARTITION_IDX, OFFSET_IDX}) == 3
