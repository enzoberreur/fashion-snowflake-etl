"""Daily refresh of slow-moving reference data only.

Decoupled from the main ETL DAG so we can schedule it less aggressively and
keep the Snowpipe-style loader on its own SLA. Reuses the same Python package.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks import on_failure

PROJECT_ROOT = Path(os.environ.get("MONOGRAM_PROJECT_ROOT", "/opt/airflow/project"))
DATA_DIR = PROJECT_ROOT / "data"


def refresh_reference_data() -> None:
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


with DAG(
    dag_id="monogram_reference_refresh",
    description="Daily Snowpipe-style refresh for reference / master data.",
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure,
        "sla": timedelta(hours=1),
    },
    schedule="0 3 * * *",  # daily at 03:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["monogram", "snowflake", "reference", "bloc3"],
) as dag:

    PythonOperator(
        task_id="refresh_reference_data",
        python_callable=refresh_reference_data,
    )
