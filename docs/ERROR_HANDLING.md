# Error handling & recovery

How the pipeline behaves under failure, and how to recover.

## 1. Failure modes by component

| Component | Failure mode | What happens | Recovery |
|-----------|--------------|---------------|----------|
| Direct ingester | One bad row in a batch | The entire `executemany` batch raises and is rolled back; the task fails. | Airflow retries (3x, exp backoff). If still failing, the source NDJSON file is the problem - pull the offending line and fix or quarantine. |
| Snowpipe ingester | `PUT` to stage fails | Local Parquet file remains; task fails. | Retry. Cleanup of orphan `TEMP_STAGE_*` happens via `monogram-check`. |
| Snowpipe ingester | `COPY` fails on a corrupt Parquet | Stage keeps the file; row count stays at zero for that batch. | Inspect `SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY` for the `ERROR_MESSAGE`. Common cause: schema drift - regenerate data or update DDL. |
| dbt `run` | A model fails | `dbt run` exits non-zero; downstream tasks (`dbt_test`, `dbt_snapshot`, `data_quality_check`) don't start. | Airflow retries the `dbt_run` task. If a model is broken, fix the SQL on a branch and redeploy DAGs. |
| dbt `test` | A test fails | Run exits non-zero. | Identify the failing test in `dbt/target/run_results.json` → fix the upstream data or update the test threshold. |
| dbt snapshot | Lock conflict | dbt waits, then errors. | Retry. Snapshot is idempotent. |
| Snowflake | Warehouse suspended | First query auto-resumes; some warehouses set `INITIALLY_SUSPENDED=TRUE`. | No action needed for auto-resume warehouses. Otherwise `ALTER WAREHOUSE INGEST RESUME;`. |
| Snowflake | Auth failure | Connector raises `DatabaseError` → task fails fast. | Verify key pair against `DESC USER` in Snowflake. See `SECURITY_NOTE.md` for the credential rotation runbook. |

## 2. Retry policy

DAG-level (defined in `dags/monogram_etl_dag.py`):

```python
DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": on_failure,
}
```

Inner-function retry decorator: `python/monogram_etl/utils/retry.py` (used selectively around network-fronted calls - currently only declared, opted-in per call site).

## 3. Callbacks

`dags/callbacks.py`:

- `on_failure(context)` - emits a structured log line (`dag_id`, `task_id`, `execution_date`, `try_number`, `exception`); if `SLACK_WEBHOOK_URL` is set, also posts a one-line Slack message with the same fields.
- `on_success(context)` - wired only on the final `notify_success` task; logs `duration` for SLA tracking.

## 4. Idempotency contracts

| Step | Idempotent? | Why |
|------|-------------|-----|
| `init_snowflake` | yes | `CREATE TABLE IF NOT EXISTS` everywhere |
| `generate_data` | yes, skip-on-existing | task checks `data/*.json` before regenerating |
| `ingest_transactional` | **no** | INSERT-only path - re-running duplicates rows. Mitigation: regenerate `data/` between runs (or implement MERGE - TIER 3 work). |
| `ingest_reference` | partly | Snowpipe-style COPY into raw, no dedup. Same caveat as above. |
| `dbt run` | yes | Tables are full-refresh by default (configurable to incremental). |
| `dbt snapshot` | yes | Only inserts changed rows. |
| `dbt test` | yes | Pure read. |
| `data_quality_check` | yes | Pure read. |

The non-idempotent ingestion steps are the obvious next refactor - switching to MERGE statements with a hash key on `sale_id` etc. would make the pipeline replay-safe.

## 5. Dead-letter handling

Today: there's no DLQ. A failing batch fails the entire task → Airflow retries → after 3 attempts the run is marked failed and the on-call gets pinged.

The TIER 3 design is: catch `RuntimeError` inside the per-line loop, write the offending JSON line + traceback to `data/_dlq/{table}_{run_id}.jsonl`, increment a counter, and continue. Then a separate task asserts `dlq_count < tolerance` and fails if breached. Not implemented yet - listed as future work.

## 6. Recovery procedures

### Re-running a failed DAG

```
Airflow UI → DAGs → monogram_etl_pipeline → failed run
  → Clear (mark Failed, then Clear)
  → DAG resumes from the first failed task
```

For ingestion replays, also clear the destination tables first to avoid duplicates:

```sql
USE DATABASE INGEST; USE SCHEMA INGEST;
TRUNCATE TABLE SALES_DATA;
TRUNCATE TABLE RETURNS_DATA;
TRUNCATE TABLE REVIEWS_DATA;
TRUNCATE TABLE INVENTORY_DATA;
```

### Cleaning up leaked temp stages

```bash
python -m monogram_etl.diagnostics.data_quality
# Look at "STAGES TEMPORAIRES RÉCENTS" output, then in Snowflake:
```

```sql
DROP STAGE TEMP_STAGE_abc12345;
```

### Restoring from a bad dbt deploy

dbt tables are full-refresh by default - re-running `dbt run` against the last-known-good commit rebuilds the marts from scratch. For the SCD2 snapshot, point-in-time recovery is possible via Snowflake Time Travel (`AT(OFFSET => -3600)`).

## 7. What's deliberately not handled

- Cross-region failover. Single Snowflake region is acceptable for a thesis.
- Real DLQ + replay (see §5).
- MERGE-based idempotent ingestion (see §4).
- Distributed tracing across Airflow → Python → dbt. Logs and query tags are the substitute.

These are listed as TIER 3 follow-ups in [`THESIS_PREP.md`](../../THESIS_PREP.md).
