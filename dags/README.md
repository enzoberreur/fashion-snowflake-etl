# dags/

Airflow DAGs for the Monogram Paris ETL.

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `monogram_etl_pipeline` | every 4h | End-to-end: init Snowflake DDL → generate data (if missing) → ingest transactional + reference → `dbt run / test / snapshot` → data-quality validation. |
| `monogram_reference_refresh` | daily 03:00 UTC | Lighter Snowpipe-style refresh for master data only (decoupled SLA). |

Both DAGs share `callbacks.py`:

- `on_failure(context)` — structured log + optional Slack webhook (`SLACK_WEBHOOK_URL`).
- `on_success(context)` — final-task SLA log.

## Deployment

```bash
# In the Airflow scheduler container / worker pod:
pip install -e /opt/airflow/project          # makes monogram_etl importable
export MONOGRAM_PROJECT_ROOT=/opt/airflow/project
export PYTHONPATH=/opt/airflow/project/dags  # so `from callbacks import ...` works

# Volume mounts:
#   /opt/airflow/project/dags  -> repo dags/
#   /opt/airflow/project/data  -> repo data/         (writable)
#   /opt/airflow/project/dbt   -> repo dbt/          (writable, profiles.yml provided via env)
#   /opt/airflow/project/sql   -> repo sql/
#   /opt/airflow/project/python -> repo python/
```

Connections required:

| Conn ID / Env var | Purpose |
|-------------------|---------|
| Env vars from `.env.example` | Snowflake account/user/key for both ingesters and dbt |
| `SLACK_WEBHOOK_URL` (optional) | Failure notifications |
| Airflow Variable `dbt_target` (default `dev`) | Which `profiles.yml` target dbt runs against |

## Topology — `monogram_etl_pipeline`

```
start
  └─► init_snowflake (DDL idempotent)
        └─► generate_data (skip if data/ populated)
              └─► [ingest_transactional, ingest_reference]   (parallel)
                    └─► dbt_deps
                          └─► dbt_run
                                ├─► dbt_test
                                ├─► dbt_snapshot         (SCD2 on dim_customer)
                                └─► data_quality_check   (sql/validation/*.sql)
                                       └─► notify_success
```

Retries: 3 with exponential backoff (2m → 4m → 8m, capped 30m). SLA: 30 minutes per task, 45-minute execution timeout.
