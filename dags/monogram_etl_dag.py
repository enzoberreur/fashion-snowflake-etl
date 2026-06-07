"""Monogram Paris — full ETL pipeline.

Topology
--------

    init_snowflake
        ├──► generate_data (optional — only when data/ is empty)
        │       ├──► ingest_transactional (sales / returns / reviews / inventory)
        │       └──► ingest_reference     (products / customers / stores / suppliers / promotions)
        │
        └──► dbt_deps ──► dbt_run ──► dbt_test
                                 └─► dbt_snapshot
                                 └─► data_quality_check ──► notify

Schedule: every 4 hours. The reference ingester runs every time but stays cheap
(small batches). The data-quality task fails loud on any breach.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from callbacks import on_failure, on_success

PROJECT_ROOT = Path(os.environ.get("MONOGRAM_PROJECT_ROOT", "/opt/airflow/project"))
DATA_DIR = PROJECT_ROOT / "data"
SQL_DDL_DIR = PROJECT_ROOT / "sql" / "ddl"
SQL_VALIDATION_DIR = PROJECT_ROOT / "sql" / "validation"
DBT_DIR = PROJECT_ROOT / "dbt"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": on_failure,
    "sla": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=45),
}


def _run_snowflake_script(script_path: Path) -> None:
    """Execute a multi-statement .sql file against Snowflake using the project connection."""
    from monogram_etl.config.snowflake import SnowflakeConnection

    with SnowflakeConnection() as sf:
        statements = [s.strip() for s in script_path.read_text().split(";") if s.strip()]
        for stmt in statements:
            if stmt.startswith("--") or not stmt:
                continue
            sf.execute_query(stmt)


def init_snowflake_task() -> None:
    for fname in ("00_database_setup.sql", "01_raw_transactional.sql", "02_raw_reference.sql"):
        _run_snowflake_script(SQL_DDL_DIR / fname)


def generate_data_task(**context) -> None:
    """Regenerate the JSON test fixtures if data/ is empty.

    In a real deployment the data would land in an SFTP drop / Kafka topic and
    this task would be replaced by a sensor.
    """
    if any(DATA_DIR.glob("*.json")):
        context["ti"].log.info("Data directory already populated; skipping generation.")
        return
    from monogram_etl.generators.data_generator import DataGenerator, GenerationConfig

    DataGenerator(GenerationConfig(output_dir=str(DATA_DIR))).generate_all()


def ingest_transactional_task() -> None:
    from monogram_etl.ingesters.direct import MultiTableIngester

    ingester = MultiTableIngester(batch_size=10_000)
    try:
        ingester.setup_tables()  # idempotent; DDL also already applied by init_snowflake
        total = 0
        for kind, path in {
            "sales": DATA_DIR / "sales.json",
            "returns": DATA_DIR / "returns.json",
            "reviews": DATA_DIR / "reviews.json",
            "inventory": DATA_DIR / "inventory.json",
        }.items():
            if not path.exists():
                continue
            method = getattr(ingester, f"ingest_{kind}_data")
            total += method(str(path))
        if total == 0:
            raise RuntimeError("No transactional rows ingested; aborting downstream tasks.")
    finally:
        ingester.sf.close()


def ingest_reference_task() -> None:
    from monogram_etl.ingesters.snowpipe import process_any_data_type

    for kind, fname in {
        "products": "products.json",
        "customers": "customers.json",
        "suppliers": "suppliers.json",
        "stores": "stores.json",
        "promotions": "promotions.json",
    }.items():
        path = DATA_DIR / fname
        if path.exists():
            process_any_data_type(str(path), kind, batch_size=2000)


def data_quality_task() -> None:
    """Run the validation SQL files; any STALE / OVER_MAX / >0 orphan row count fails."""
    from monogram_etl.config.snowflake import SnowflakeConnection

    failures: list[str] = []
    with SnowflakeConnection() as sf:
        # Freshness
        for row in sf.execute_query((SQL_VALIDATION_DIR / "assert_data_freshness.sql").read_text()):
            table_name, _, _, _, status = row
            if status in ("STALE", "EMPTY"):
                failures.append(f"freshness: {table_name} is {status}")
        # Referential integrity
        for row in sf.execute_query((SQL_VALIDATION_DIR / "assert_referential_integrity.sql").read_text()):
            check_name, offending = row
            if offending and offending > 0:
                failures.append(f"referential: {check_name}={offending}")
        # Row-count bounds
        for row in sf.execute_query((SQL_VALIDATION_DIR / "assert_row_count_within_bounds.sql").read_text()):
            table_name, actual, _, _, status = row
            if status != "OK":
                failures.append(f"row_count: {table_name} actual={actual} status={status}")

    if failures:
        raise RuntimeError("Data quality breaches:\n  - " + "\n  - ".join(failures))


with DAG(
    dag_id="monogram_etl_pipeline",
    description="Monogram Paris — generate → ingest → transform (dbt) → test → snapshot",
    default_args=DEFAULT_ARGS,
    schedule="0 */4 * * *",  # every 4 hours
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["monogram", "snowflake", "dbt", "etl", "bloc3"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    init_snowflake = PythonOperator(
        task_id="init_snowflake",
        python_callable=init_snowflake_task,
        doc_md="Apply sql/ddl/00..02 to Snowflake. Idempotent (CREATE IF NOT EXISTS).",
    )

    generate = PythonOperator(
        task_id="generate_data",
        python_callable=generate_data_task,
        doc_md="Populate data/*.json from the Faker generator if missing.",
    )

    with TaskGroup(group_id="ingest") as ingest_group:
        ingest_transactional = PythonOperator(
            task_id="ingest_transactional",
            python_callable=ingest_transactional_task,
            doc_md="Direct SQL INSERT path for sales/returns/reviews/inventory.",
        )
        ingest_reference = PythonOperator(
            task_id="ingest_reference",
            python_callable=ingest_reference_task,
            doc_md="Parquet + COPY path for products/customers/stores/suppliers/promotions.",
        )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --quiet",
        doc_md="Install dbt_utils and dbt_expectations.",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --target {{{{ var.value.get('dbt_target', 'dev') }}}}",
        doc_md="Build staging views + marts tables.",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --target {{{{ var.value.get('dbt_target', 'dev') }}}}",
        doc_md="Run schema.yml tests + custom singular tests.",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && dbt snapshot --target {{{{ var.value.get('dbt_target', 'dev') }}}}",
        doc_md="Capture SCD2 history for the customer dimension.",
    )

    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_task,
        doc_md="Run sql/validation/*.sql — freshness, referential integrity, row-count bounds.",
    )

    notify = EmptyOperator(
        task_id="notify_success",
        on_success_callback=on_success,
        doc_md="Sentinel that fires the success callback (logging + optional Slack).",
    )

    (
        start
        >> init_snowflake
        >> generate
        >> ingest_group
        >> dbt_deps
        >> dbt_run
        >> [dbt_test, dbt_snapshot, data_quality_check]
        >> notify
    )
