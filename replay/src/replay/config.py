"""Replay service settings via pydantic-settings."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    kafka_bootstrap_servers: str = Field(default="localhost:19092", alias="KAFKA_BOOTSTRAP_SERVERS")
    iceberg_rest_uri: str = Field(default="http://localhost:8181", alias="ICEBERG_REST_URI")
    iceberg_warehouse: str = Field(default="s3://ticksense/warehouse", alias="ICEBERG_WAREHOUSE")
    s3_endpoint: str = Field(default="http://localhost:9000", alias="S3_ENDPOINT")
    s3_access_key: str = Field(default="minioadmin", alias="AWS_ACCESS_KEY_ID")
    s3_secret_key: str = Field(default="minioadmin", alias="AWS_SECRET_ACCESS_KEY")

    # Topic that contains the raw L2 order book events to replay.
    source_topic: str = Field(default="market.raw.orderbook", alias="REPLAY_SOURCE_TOPIC")
    target_topic: str = Field(default="market.raw.orderbook", alias="REPLAY_TARGET_TOPIC")

    # Consumer group used only for reading (not committed during replay).
    replay_consumer_group: str = Field(
        default="replay-consumer-temp", alias="REPLAY_CONSUMER_GROUP"
    )
    # Iceberg catalog + table that holds kafka_offset metadata.
    iceberg_namespace: str = Field(default="normalized", alias="REPLAY_ICEBERG_NAMESPACE")
    iceberg_table: str = Field(default="book_ticker", alias="REPLAY_ICEBERG_TABLE")
