"""Unit tests for the Snowpipe Streaming sink mapping + profile (no Snowflake)."""
from __future__ import annotations

import json

import pytest

from monogram_etl.streaming.sink import STREAM_COLUMNS, event_to_row, write_profile_json

pytestmark = pytest.mark.unit

_EVENT = {
    "event_id": "e1", "sale_id": "S1", "event_ts": "2026-06-07T10:00:00+00:00",
    "sale_date": "2026-06-07", "customer_id": "C1", "product_id": "P1",
    "product_name": "Dior Veste", "quantity": 2, "unit_price": 10.0, "total_amount": 20.0,
    "channel": "Boutique", "store_id": "ST1", "country": "France",
}


def test_event_to_row_keys_match_table_columns():
    row = event_to_row(_EVENT, partition=1, offset=42)
    assert set(row) == set(STREAM_COLUMNS)
    assert row["KAFKA_PARTITION"] == 1
    assert row["KAFKA_OFFSET"] == 42
    assert row["INGESTED_AT"]  # consumer-side land time


def test_event_to_row_preserves_business_values():
    row = event_to_row(_EVENT, partition=0, offset=0)
    assert row["SALE_ID"] == "S1"
    assert row["TOTAL_AMOUNT"] == 20.0
    assert row["PRODUCT_NAME"] == "Dior Veste"


def test_write_profile_json(tmp_path, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "ACC123")
    monkeypatch.setenv("SNOWFLAKE_USER", "USR")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PATH", "keys/rsa_key.p8")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "INGEST")
    path = tmp_path / "profile.json"
    write_profile_json(str(path))
    profile = json.loads(path.read_text())
    assert profile["account"] == "ACC123"
    assert profile["user"] == "USR"
    assert profile["private_key_file"] == "keys/rsa_key.p8"
    assert profile["role"] == "INGEST"
    assert profile["url"] == "https://ACC123.snowflakecomputing.com:443"
