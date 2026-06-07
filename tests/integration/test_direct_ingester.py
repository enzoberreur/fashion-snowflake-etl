"""Mock-based integration tests for the direct ingester.

These avoid Snowflake entirely by patching SnowflakeConnection at import time.
We verify batching, SQL parameterisation, and total row counts.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def fake_sf() -> MagicMock:
    sf = MagicMock()
    sf.execute_query = MagicMock()
    sf.execute_batch = MagicMock()
    sf.close = MagicMock()
    return sf


def _write_ndjson(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def test_setup_tables_creates_four_transactional_tables(fake_sf: MagicMock) -> None:
    from monogram_etl.ingesters.direct import MultiTableIngester

    with patch("monogram_etl.ingesters.direct.SnowflakeConnection", return_value=fake_sf):
        ingester = MultiTableIngester(batch_size=100)
        ingester.setup_tables()

    queries = [c.args[0] for c in fake_sf.execute_query.call_args_list]
    create_statements = [q for q in queries if "CREATE OR REPLACE TABLE" in q]
    assert len(create_statements) == 4
    table_names = {q.split()[4] for q in create_statements}
    assert table_names == {"SALES_DATA", "RETURNS_DATA", "REVIEWS_DATA", "INVENTORY_DATA"}


def test_ingest_sales_batches_respect_size(tmp_path: Path, fake_sf: MagicMock) -> None:
    sales_file = tmp_path / "sales.json"
    records = [
        {
            "sale_id": f"S{i:05d}",
            "sale_date": "2024-01-01",
            "customer_id": f"C{(i % 5) + 1:05d}",
            "product_id": f"P{(i % 3) + 1:05d}",
            "product_name": "Item",
            "quantity": 1,
            "unit_price": 10.0,
            "total_amount": 10.0,
            "channel": "online",
            "store_id": "ST001",
            "country": "France",
        }
        for i in range(25)
    ]
    _write_ndjson(sales_file, records)

    from monogram_etl.ingesters.direct import MultiTableIngester

    with patch("monogram_etl.ingesters.direct.SnowflakeConnection", return_value=fake_sf):
        ingester = MultiTableIngester(batch_size=10)
        total = ingester.ingest_sales_data(str(sales_file))

    assert total == 25
    # With batch_size=10 and 25 rows we expect 3 execute_batch calls: 10, 10, 5
    batch_sizes = [len(call.args[1]) for call in fake_sf.execute_batch.call_args_list]
    assert batch_sizes == [10, 10, 5]


def test_ingest_sales_handles_empty_file(tmp_path: Path, fake_sf: MagicMock) -> None:
    empty = tmp_path / "sales_empty.json"
    empty.write_text("")

    from monogram_etl.ingesters.direct import MultiTableIngester

    with patch("monogram_etl.ingesters.direct.SnowflakeConnection", return_value=fake_sf):
        ingester = MultiTableIngester(batch_size=10)
        total = ingester.ingest_sales_data(str(empty))

    assert total == 0
    fake_sf.execute_batch.assert_not_called()
