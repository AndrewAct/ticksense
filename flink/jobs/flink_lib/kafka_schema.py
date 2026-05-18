"""
Kafka record helpers shared across Flink jobs and unit tests.

parse_kafka_record is importable without PyFlink (used in unit tests).
KAFKA_RECORD_TYPE is only defined when PyFlink is present; the try/except
keeps the module importable in the test environment.

Real partition and offset are captured via Flink SQL Kafka source metadata
columns ('partition' VIRTUAL, 'offset' VIRTUAL) and bridged to DataStream
with to_append_stream().  This is the supported path in PyFlink 1.18; the
Python bindings do not expose KafkaRecordDeserializationSchema.
"""

from __future__ import annotations

# Positional indices into Row(payload, kafka_partition, kafka_offset).
PAYLOAD_IDX = 0
PARTITION_IDX = 1
OFFSET_IDX = 2


def parse_kafka_record(record: object) -> tuple[str, int, int] | None:
    """Extract (payload_str, partition, offset) from a Kafka ConsumerRecord.

    Duck-typed: any object with .value(), .partition(), .offset() methods works,
    making this testable without PyFlink.

    Returns None for tombstone records (null value bytes).
    """
    value_bytes = record.value()  # type: ignore[union-attr]
    if value_bytes is None:
        return None
    payload = bytes(value_bytes).decode("utf-8")
    partition: int = record.partition()  # type: ignore[union-attr]
    offset: int = record.offset()  # type: ignore[union-attr]
    return (payload, partition, offset)


try:
    from pyflink.common.typeinfo import Types

    KAFKA_RECORD_TYPE = Types.ROW_NAMED(
        ["payload", "kafka_partition", "kafka_offset"],
        [Types.STRING(), Types.INT(), Types.LONG()],
    )
except ImportError:
    pass
