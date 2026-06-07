"""Pipeline monitoring queries.

Run interactively (`python -m monogram_etl.diagnostics.monitoring`) or as part of
an Airflow task to surface:

- Recent COPY history (success vs failure rates).
- Per-table ingestion lag.
- Long-running queries on the ingestion warehouse.

Complements ``sql/validation/*.sql`` (data-quality checks) by focusing on
operational health rather than business correctness.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from monogram_etl.config.snowflake import SnowflakeConnection
from monogram_etl.utils.logging import get_logger

logger = get_logger(__name__)

_COPY_HISTORY_SQL = """
select
    table_name,
    file_name,
    status,
    row_count,
    error_count,
    pipe_received_time,
    last_load_time,
    datediff('minute', pipe_received_time, last_load_time) as load_minutes
from snowflake.account_usage.copy_history
where pipe_received_time >= dateadd('hour', -24, current_timestamp())
order by pipe_received_time desc
limit 100
""".strip()

_QUERY_HISTORY_FAILURES_SQL = """
select
    query_id,
    user_name,
    warehouse_name,
    execution_status,
    error_code,
    error_message,
    start_time,
    total_elapsed_time / 1000.0 as elapsed_seconds
from snowflake.account_usage.query_history
where start_time >= dateadd('hour', -24, current_timestamp())
  and execution_status in ('FAIL', 'INCIDENT')
  and warehouse_name = 'INGEST'
order by start_time desc
limit 50
""".strip()

_INGESTION_LAG_SQL = """
select
    'SALES_DATA'              as table_name, max(CREATED_AT) as last_loaded_at from SALES_DATA
union all select 'RETURNS_DATA',            max(CREATED_AT)                       from RETURNS_DATA
union all select 'REVIEWS_DATA',            max(CREATED_AT)                       from REVIEWS_DATA
union all select 'INVENTORY_DATA',          max(CREATED_AT)                       from INVENTORY_DATA
""".strip()


@dataclass
class CopyHistoryRow:
    table_name: str
    file_name: str
    status: str
    row_count: int
    error_count: int


def fetch_copy_history(limit_hours: int = 24) -> Iterable[CopyHistoryRow]:
    """Last `limit_hours` of COPY operations across the INGEST account."""
    with SnowflakeConnection(query_tag="monogram-monitoring") as sf:
        rows = sf.execute_query(_COPY_HISTORY_SQL)
    for r in rows:
        yield CopyHistoryRow(
            table_name=r[0],
            file_name=r[1],
            status=r[2],
            row_count=r[3] or 0,
            error_count=r[4] or 0,
        )


def fetch_recent_query_failures() -> list[tuple]:
    """Recent FAIL/INCIDENT queries on the INGEST warehouse."""
    with SnowflakeConnection(query_tag="monogram-monitoring") as sf:
        return sf.execute_query(_QUERY_HISTORY_FAILURES_SQL)


def fetch_ingestion_lag() -> list[tuple]:
    """Most recent CREATED_AT per transactional table."""
    with SnowflakeConnection(query_tag="monogram-monitoring") as sf:
        return sf.execute_query(_INGESTION_LAG_SQL)


def main() -> None:
    print("=== Ingestion lag ===")
    for row in fetch_ingestion_lag():
        print(f"  {row[0]:30s} last load: {row[1]}")

    print("\n=== Recent COPY history (last 24h) ===")
    for r in fetch_copy_history():
        print(f"  [{r.status:10s}] {r.table_name:30s} rows={r.row_count} errors={r.error_count} file={r.file_name}")

    print("\n=== Recent FAIL/INCIDENT queries (last 24h) ===")
    for row in fetch_recent_query_failures():
        print(f"  [{row[3]:10s}] {row[5][:80]}... ({row[7]:.1f}s)")


if __name__ == "__main__":
    main()
