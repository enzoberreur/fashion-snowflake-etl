"""Monogram Paris - real-time stream monitor.

Runs every 5 minutes to watch the Snowpipe Streaming path independently of the
long-running consumer service:

    stream_quality_check (freshness + exactly-once + throughput SQL)
        └──► dbt_source_freshness (STREAM_SALES) ──► notify

Fails loud on a STALE stream or any exactly-once (duplicate offset) breach, so a
stuck or double-writing consumer is caught quickly. Throughput/latency are logged.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from callbacks import on_failure, on_success

PROJECT_ROOT = Path(os.environ.get("MONOGRAM_PROJECT_ROOT", "/opt/airflow/project"))
SQL_VALIDATION_DIR = PROJECT_ROOT / "sql" / "validation"
DBT_DIR = PROJECT_ROOT / "dbt"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_failure,
    "execution_timeout": timedelta(minutes=10),
}


def stream_quality_task(**context) -> None:
    """Run the streaming validations; fail on STALE freshness or duplicate offsets."""
    from monogram_etl.config.snowflake import SnowflakeConnection

    log = context["ti"].log
    failures: list[str] = []
    with SnowflakeConnection(query_tag="stream-monitor") as sf:
        # Freshness
        for table_name, last_loaded, _sla, mins, rows, status in sf.execute_query(
            (SQL_VALIDATION_DIR / "assert_stream_freshness.sql").read_text()
        ):
            log.info("freshness: %s status=%s rows=%s mins_since_load=%s", table_name, status, rows, mins)
            if status == "STALE":
                failures.append(f"stream freshness: {table_name} is STALE ({mins} min since last event)")

        # Exactly-once
        for check_name, offending in sf.execute_query(
            (SQL_VALIDATION_DIR / "assert_stream_exactly_once.sql").read_text()
        ):
            log.info("exactly-once: %s offending_groups=%s", check_name, offending)
            if offending and offending > 0:
                failures.append(f"exactly-once breach: {offending} duplicated (partition, offset) groups")

        # Throughput (informational)
        for events, avg_lat, max_lat, parts in sf.execute_query(
            (SQL_VALIDATION_DIR / "assert_stream_throughput.sql").read_text()
        ):
            log.info("throughput: %s events/15min, avg_latency=%ss max_latency=%ss partitions=%s",
                     events, avg_lat, max_lat, parts)

    if failures:
        raise RuntimeError("Streaming quality breaches:\n  - " + "\n  - ".join(failures))


with DAG(
    dag_id="monogram_stream_monitor",
    description="Monogram Paris - monitor the real-time Snowpipe Streaming path (freshness, exactly-once, throughput)",
    default_args=DEFAULT_ARGS,
    schedule="*/5 * * * *",  # every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["monogram", "snowflake", "streaming", "monitoring", "bloc3"],
    doc_md=__doc__,
) as dag:

    stream_quality_check = PythonOperator(
        task_id="stream_quality_check",
        python_callable=stream_quality_task,
        doc_md="Freshness + exactly-once + throughput on STREAM_SALES (sql/validation/assert_stream_*.sql).",
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            f"cd {DBT_DIR} && dbt source freshness "
            f"--select source:monogram_raw.STREAM_SALES "
            f"--target {{{{ var.value.get('dbt_target', 'dev') }}}}"
        ),
        doc_md="dbt-native freshness check on the STREAM_SALES source.",
    )

    notify = EmptyOperator(task_id="notify_success", on_success_callback=on_success)

    stream_quality_check >> dbt_source_freshness >> notify
