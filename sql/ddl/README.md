# sql/ddl

Raw-layer DDL extracted from the Python ingesters. Run order matters:

| File | Purpose | Owner |
|------|---------|-------|
| `00_database_setup.sql` | Create warehouse, database, schemas (raw/staging/marts), role, grants. One-time setup. | SYSADMIN / SECURITYADMIN |
| `01_raw_transactional.sql` | Raw tables for the direct ingester: `SALES_DATA`, `RETURNS_DATA`, `REVIEWS_DATA`, `INVENTORY_DATA`. | INGEST role |
| `02_raw_reference.sql` | Raw tables for the snowpipe ingester: `PRODUCTS_DATA_SNOWPIPE`, `CUSTOMERS_DATA_SNOWPIPE`, `SUPPLIERS_DATA_SNOWPIPE`, `STORES_DATA_SNOWPIPE`, `PROMOTIONS_DATA_SNOWPIPE`. | INGEST role |

All DDL is idempotent (`CREATE TABLE IF NOT EXISTS`). The Python ingesters previously embedded these statements; they now defer to these files via the `MONOGRAM_RUN_DDL` env var or the Airflow `init_snowflake` task.

Apply manually:

```bash
snowsql -f sql/ddl/00_database_setup.sql
snowsql -f sql/ddl/01_raw_transactional.sql
snowsql -f sql/ddl/02_raw_reference.sql
```

Or via the Airflow DAG `monogram_etl_pipeline`, which runs `init_snowflake` at the start of every DAG run.

Downstream layers (`staging/`, `marts/`) live in `/dbt/models/`.
