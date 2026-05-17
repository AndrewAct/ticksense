"""
Kafka record deserialization helpers.

The pure function parse_kafka_record is importable without PyFlink (used in
unit tests running on Python 3.12 in the root workspace).  The
KafkaRecordDeserializer class and KAFKA_RECORD_TYPE are only defined when
PyFlink is present; they live under a try/except so the module remains
importable in the test environment.

Why this exists
---------------
SimpleStringSchema is a value-only deserializer: it discards all Kafka record
metadata (partition, offset, timestamp, key).  Switching to
KafkaRecordDeserializationSchema lets downstream Flink jobs store the real
kafka_partition and kafka_offset in Iceberg instead of -1, enabling precise
offset-based seek during replay.
"""

from __future__ import annotations

# Positional indices into the Row emitted by KafkaRecordDeserializer.
# Use these constants instead of magic numbers in job code.
PAYLOAD_IDX = 0
PARTITION_IDX = 1
OFFSET_IDX = 2


def parse_kafka_record(record: object) -> tuple[str, int, int] | None:
    """Extract (payload_str, partition, offset) from a Kafka ConsumerRecord.

    Duck-typed: any object with .value(), .partition(), .offset() methods works,
    which makes this function testable without PyFlink.

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
    from pyflink.common import Row
    from pyflink.common.typeinfo import Types
    from pyflink.datastream.connectors.kafka import KafkaRecordDeserializationSchema

    KAFKA_RECORD_TYPE = Types.ROW_NAMED(
        ["payload", "kafka_partition", "kafka_offset"],
        [Types.STRING(), Types.INT(), Types.LONG()],
    )

    class KafkaRecordDeserializer(KafkaRecordDeserializationSchema):
        """Replaces SimpleStringSchema to capture real kafka_partition and kafka_offset.

        Emits Row(payload: str, kafka_partition: int, kafka_offset: long).
        Tombstone records (null value) are suppressed here so downstream
        processors never receive None payloads.
        """

        def deserialize(self, record: object, collector: object) -> None:
            result = parse_kafka_record(record)
            if result is not None:
                payload, partition, offset = result
                collector.collect(Row(payload, partition, offset))  # type: ignore[union-attr]

        def get_produced_type(self) -> object:
            return KAFKA_RECORD_TYPE

except ImportError:
    pass
