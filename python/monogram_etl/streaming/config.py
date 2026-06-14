"""Configuration for the Monogram real-time streaming layer.

Kafka/Redpanda connection and the Snowflake Snowpipe Streaming target, both
driven by environment variables with sensible local-docker defaults so the
producer and consumer run with zero config against the bundled docker-compose.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

# Redpanda's external (host) listener from the bundled docker-compose.
# Inside the compose network, set KAFKA_BOOTSTRAP_SERVERS=redpanda:9092.
DEFAULT_BOOTSTRAP = "localhost:19092"
DEFAULT_TOPIC = "monogram.sales.stream"
DEFAULT_GROUP = "monogram-stream-consumer"


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = DEFAULT_BOOTSTRAP
    topic: str = DEFAULT_TOPIC
    group_id: str = DEFAULT_GROUP

    @classmethod
    def from_env(cls) -> KafkaSettings:
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP),
            topic=os.getenv("KAFKA_SALES_TOPIC", DEFAULT_TOPIC),
            group_id=os.getenv("KAFKA_CONSUMER_GROUP", DEFAULT_GROUP),
        )


@dataclass(frozen=True)
class SnowpipeSettings:
    """Target for the Snowpipe Streaming consumer (high-performance, PIPE-based)."""

    database: str = "INGEST"
    schema: str = "INGEST"
    table: str = "STREAM_SALES"
    pipe: str = "STREAM_SALES_PIPE"
    channel_prefix: str = "monogram"

    @classmethod
    def from_env(cls) -> SnowpipeSettings:
        return cls(
            database=os.getenv("SNOWFLAKE_DATABASE", "INGEST"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "INGEST"),
            table=os.getenv("STREAM_TABLE", "STREAM_SALES"),
            pipe=os.getenv("STREAM_PIPE", "STREAM_SALES_PIPE"),
            channel_prefix=os.getenv("STREAM_CHANNEL_PREFIX", "monogram"),
        )
