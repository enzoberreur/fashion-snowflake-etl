# Monitoring & observability

What's watched, where to look, and when to page.

## 1. Coverage matrix

| Component | Source of truth | What's monitored | Alert threshold | Where to look |
|-----------|-----------------|------------------|-----------------|---------------|
| Airflow scheduler | Airflow UI + scheduler logs | Heartbeat, DAG run latency, queued tasks | Heartbeat > 60s late | `http://airflow-host:8080` |
| Direct ingester | Snowflake `QUERY_HISTORY` (tag `monogram-direct`) | Row count per batch, INSERT latency, errors | Any FAIL → DAG fails | `diagnostics/monitoring.py` `fetch_recent_query_failures()` |
| Snowpipe ingester | Snowflake `COPY_HISTORY` (tag `monogram-snowpipe`) | File counts, rows loaded, errors, stage cleanup | Any FAIL → DAG fails; orphan `TEMP_STAGE_*` | `diagnostics/monitoring.py` `fetch_copy_history()` |
| Raw freshness | `sql/validation/assert_data_freshness.sql` | `max(CREATED_AT)` per raw table | Transactional: > 6h stale = warn; > 24h = error. Reference: 7d / 30d | Airflow `data_quality_check` task |
| Referential integrity | `sql/validation/assert_referential_integrity.sql` | Orphan FKs across raw tables | Any orphan row > 0 = error | Airflow `data_quality_check` task |
| Row counts | `sql/validation/assert_row_count_within_bounds.sql` | Row count vs expected band | UNDER_MIN or OVER_MAX = error | Airflow `data_quality_check` task |
| Star schema | dbt `schema.yml` tests | Uniqueness, nullness, relationships, accepted values, numeric bounds | Failure = `dbt build` exits non-zero | Airflow `dbt_test` task; `dbt/target/run_results.json` |
| Singular dbt tests | `dbt/tests/*.sql` | Business invariants (amount consistency, no future dates) | Any row returned = failure | Airflow `dbt_test` task |
| Snowflake warehouse | `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` | Credits consumed, suspend lag | Daily credits > budget | Snowsight |
| Snowflake auth | `SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY` | Failed logins | > 5 in 1 hour = error | Snowsight |

## 2. Standard ops queries

```bash
# Operational health snapshot
python -m monogram_etl.diagnostics.monitoring

# Table content + recent COPY history
python -m monogram_etl.diagnostics.data_quality

# Ad-hoc DQ
snowsql -f sql/validation/assert_data_freshness.sql
snowsql -f sql/validation/assert_referential_integrity.sql
snowsql -f sql/validation/assert_row_count_within_bounds.sql
```

## 3. Alert routing

| Severity | Trigger | Where it lands |
|----------|---------|----------------|
| `INFO` | Successful DAG completion | `on_success_callback` → structured log line in Airflow |
| `WARN` | dbt test with `severity: warn` (e.g. `accepted_values`) | dbt run summary; visible in Airflow `dbt_test` logs |
| `ERROR` | Task fails after retries (3 attempts, exponential backoff) | `on_failure_callback` → structured log + optional Slack via `SLACK_WEBHOOK_URL` |

The Slack webhook is opt-in. Without it, failures still surface via the Airflow UI and the structured logs (greppable with `dag_id=monogram_etl_pipeline level=ERROR`).

## 4. SLOs

| Metric | Objective | Measured how |
|--------|-----------|--------------|
| End-to-end DAG latency | < 30 min per run | Airflow SLA |
| Per-task latency | < 45 min hard timeout | Airflow `execution_timeout` |
| Raw transactional freshness | `max(CREATED_AT)` ≤ 6 h | `assert_data_freshness.sql` |
| dbt test pass rate | 100 % of `severity: error` tests | `dbt test` exit code |
| Successful runs | ≥ 95 % over 7 days | Airflow + `QUERY_HISTORY` failed-query count |

## 5. Dashboard ideas (not built — future work)

A small Streamlit / Superset on top of Snowflake would surface:

- Sales volume by day, with returns overlay
- Customer value-tier drift (% VIP vs Inactive over time)
- Per-source freshness heatmap (table × hour)
- COPY error rate (last 24 h)
- Warehouse credit burn rate

`docs/DATA_MODEL.md` lists the analytical entities to query.

## 6. Runbook — "data quality check failed"

1. Open the Airflow `data_quality_check` task log → identify which assertion failed.
2. If **freshness**: check the relevant ingester task above it in the DAG. Common causes — Snowflake warehouse suspended (try `ALTER WAREHOUSE INGEST RESUME;`), key auth expired, source data missing.
3. If **referential integrity**: probably an ingestion-order race. `sql/validation/assert_referential_integrity.sql` reports per-check counts; run `monogram-check` for samples.
4. If **row-count bounds**: someone changed generator config without updating `assert_row_count_within_bounds.sql`. Update the expected band or re-run the generator with the documented settings.
5. After fix: clear the failed DAG run (Airflow UI → Mark failed → Clear) so it retries from `data_quality_check`.
