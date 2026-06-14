# dbt - Monogram Paris

Transformation layer that turns the raw `INGEST.INGEST` tables into a star-schema in `INGEST.MARTS`.

## Layers

```
INGEST  (raw)      ──►  STAGING  (views, light cast + rename)  ──►  MARTS  (tables, conformed star schema)
```

- **Sources** - `models/sources.yml` declares every raw table loaded by the Python ingesters.
- **Staging** - `models/staging/stg_*.sql`, one view per raw table. Casts types, renames to `snake_case`, drops obviously bad rows (nulls in PK).
- **Marts** - `models/marts/`. Conformed dimensions (`dim_customer`, `dim_product`, `dim_store`, `dim_date`) and facts (`fact_sales`, `fact_returns`).

## Star schema (high level)

```
                              ┌──────────────┐
                              │   dim_date   │
                              └──────┬───────┘
                                     │
┌──────────────┐   ┌─────────────┐   │   ┌──────────────┐   ┌──────────────┐
│ dim_customer ├───┤ fact_sales  ├───┴───┤ fact_returns ├───┤ dim_product  │
└──────────────┘   └──────┬──────┘       └──────┬───────┘   └──────────────┘
                          │                     │
                    ┌─────┴──────┐        ┌─────┴──────┐
                    │ dim_store  │        │ dim_store  │
                    └────────────┘        └────────────┘
```

`fact_sales` and `fact_returns` are conformed on `dim_customer`, `dim_product`, `dim_store`, and `dim_date`.

## Running

```bash
# Install dependencies
cd dbt
dbt deps

# Build everything
dbt build --target dev          # run + test in one go
# Or step by step:
dbt run --target dev
dbt test --target dev
```

The Airflow DAG `monogram_etl_pipeline` invokes `dbt build` as its final task.

## Tests

Tests live in `models/**/schema.yml`. They cover:

- **`not_null` / `unique`** on every primary key.
- **`relationships`** between facts and dimensions (referential integrity).
- **`accepted_values`** on enum-like columns (`status`, `channel`, `discount_type`).
- **`dbt_expectations.expect_column_values_to_be_between`** on numeric bounds (rating ∈ [1, 5], quantity > 0).
- **Freshness** - sources declare `loaded_at_field: CREATED_AT` so `dbt source freshness` flags stale data.
