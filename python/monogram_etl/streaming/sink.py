"""Snowpipe Streaming sink: lands sales events into Snowflake via the high-performance SDK.

Uses the auto-created default pipe ``<TABLE>-STREAMING`` with MATCH_BY_COLUMN_NAME,
so we append row dicts whose keys match the STREAM_SALES columns - no explicit
CREATE PIPE needed. Exactly-once is provided by per-channel offset tokens (the
consumer uses the Kafka partition offset), which let the channel resume from the
last committed position after a restart.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from monogram_etl.streaming.config import SnowpipeSettings

logger = logging.getLogger("monogram.stream.sink")

# Columns in INGEST.INGEST.STREAM_SALES; row-dict keys are matched to these by name.
STREAM_COLUMNS = [
    "EVENT_ID", "SALE_ID", "EVENT_TS", "SALE_DATE", "CUSTOMER_ID", "PRODUCT_ID",
    "PRODUCT_NAME", "QUANTITY", "UNIT_PRICE", "TOTAL_AMOUNT", "CHANNEL", "STORE_ID",
    "COUNTRY", "KAFKA_PARTITION", "KAFKA_OFFSET", "INGESTED_AT",
]


def write_profile_json(path: str = "profile.json") -> str:
    """Render the Snowpipe Streaming profile.json from the same env vars as the connector."""
    account = os.environ["SNOWFLAKE_ACCOUNT"]
    profile = {
        "user": os.environ["SNOWFLAKE_USER"],
        "account": account,
        "url": os.getenv("SNOWFLAKE_URL", f"https://{account}.snowflakecomputing.com:443"),
        "private_key_file": os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        "role": os.getenv("SNOWFLAKE_ROLE", "INGEST"),
    }
    Path(path).write_text(json.dumps(profile, indent=2), encoding="utf-8")
    return path


def event_to_row(
    event: dict[str, Any], *, partition: int = 0, offset: int = 0, ingested_at: str | None = None
) -> dict[str, Any]:
    """Map a wire event (lowercase keys) to a STREAM_SALES row (column-name keys)."""
    return {
        "EVENT_ID": event["event_id"],
        "SALE_ID": event["sale_id"],
        "EVENT_TS": event["event_ts"],
        "SALE_DATE": event["sale_date"],
        "CUSTOMER_ID": event["customer_id"],
        "PRODUCT_ID": event["product_id"],
        "PRODUCT_NAME": event["product_name"],
        "QUANTITY": event["quantity"],
        "UNIT_PRICE": event["unit_price"],
        "TOTAL_AMOUNT": event["total_amount"],
        "CHANNEL": event["channel"],
        "STORE_ID": event["store_id"],
        "COUNTRY": event["country"],
        "KAFKA_PARTITION": partition,
        "KAFKA_OFFSET": offset,
        "INGESTED_AT": ingested_at or datetime.now(timezone.utc).isoformat(),
    }


class SnowpipeStreamingSink:
    """Thin wrapper over StreamingIngestClient/Channel for one STREAM_SALES channel."""

    def __init__(
        self,
        settings: SnowpipeSettings | None = None,
        *,
        client_name: str = "monogram-stream",
        channel_name: str = "monogram-0",
        profile_path: str = "profile.json",
    ):
        self.settings = settings or SnowpipeSettings.from_env()
        self.pipe_name = f"{self.settings.table}-STREAMING"  # default pipe convention
        self.client_name = client_name
        self.channel_name = channel_name
        self._profile_path = profile_path
        self._client: Any = None
        self._channel: Any = None

    def open(self) -> str | None:
        from snowflake.ingest.streaming import StreamingIngestClient

        write_profile_json(self._profile_path)
        self._client = StreamingIngestClient(
            self.client_name,
            self.settings.database,
            self.settings.schema,
            self.pipe_name,
            profile_json=self._profile_path,
        )
        self._channel, _status = self._client.open_channel(self.channel_name)
        committed = self._channel.get_latest_committed_offset_token()
        logger.info(
            "Opened channel '%s' on %s.%s.%s (last committed offset=%s)",
            self.channel_name, self.settings.database, self.settings.schema, self.pipe_name, committed,
        )
        return committed

    def append(self, row: dict[str, Any], offset_token: int | str) -> None:
        """Append one row tagged with an offset token (required for commit + exactly-once)."""
        self._channel.append_row(row, str(offset_token))

    def append_batch(self, rows_with_offsets: list[tuple[int, dict[str, Any]]]) -> None:
        """Append rows, each tagged with its (monotonic) offset token."""
        for offset, row in rows_with_offsets:
            self._channel.append_row(row, str(offset))

    def latest_committed_offset(self) -> str | None:
        return self._channel.get_latest_committed_offset_token() if self._channel else None

    def flush(self, timeout_seconds: int = 60) -> None:
        """Block until all appended rows are flushed to the table."""
        if self._channel:
            self._channel.wait_for_flush(timeout_seconds=timeout_seconds)

    def wait_committed(self, min_offset: int, timeout_seconds: int = 60) -> None:
        """Block until the committed offset token reaches min_offset (exactly-once high-water mark)."""

        def committed(token: str | None) -> bool:
            return token not in (None, "") and int(token) >= min_offset

        self._channel.wait_for_commit(committed, timeout_seconds=timeout_seconds)

    def close(self) -> None:
        try:
            if self._channel is not None:
                self._channel.close()
        finally:
            if self._client is not None:
                self._client.close()

    def __enter__(self) -> "SnowpipeStreamingSink":
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
