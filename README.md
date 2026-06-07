# 👑 Monogram Paris — ETL Pipeline

**Fashion Vintage Luxury** — Snowflake-backed ETL for authentication and sales of collectible fashion pieces (Chanel, Dior, Hermès, YSL).

Two ingestion paths (real-time SQL + Parquet/COPY) feed a dbt-built star schema in Snowflake. Airflow orchestrates the full loop; dbt + dbt_expectations enforce data quality.

> **Thesis context:** this repository is Bloc 3 (Pipelines de Données Temps Réel) of the data engineering thesis. See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full deliverable map.

---

## 👥 Team

* **Enzo Berreur** — Data Engineer
* **Sara Ben Abdelkader** — Data Analyst / ETL
* **Antonin Arroyo** — Back-end Developer
* **Nehemie Bikuka Prince** — Data Engineer

---

## 🏛️ Repository layout

```
.
├── dags/                      Airflow DAGs (orchestration)
├── sql/
│   ├── ddl/                   Raw-layer DDL (replayable, idempotent)
│   └── validation/            Operational DQ assertions (freshness, FK, row counts)
├── python/monogram_etl/       Python package (installed via pyproject.toml)
│   ├── config/                SnowflakeConnection (single auth code path)
│   ├── generators/            Faker-based test data generator
│   ├── ingesters/             direct.py (SQL INSERT) + snowpipe.py (Parquet + COPY)
│   ├── diagnostics/           data_quality.py + monitoring.py
│   └── utils/                 logging, retry
├── dbt/                       Transformation layer (staging → marts star schema)
├── tests/                     pytest suite (unit / integration / data_quality)
├── docs/                      Architecture, data model, monitoring, error handling
├── data/                      Generated NDJSON (gitignored)
├── .env.example               Credential template
└── SECURITY_NOTE.md           Disclosure of a past credential exposure (account dead)
```

---

## 🚀 Quick start

```bash
# 1. Install
python -m venv .venv && source .venv/bin/activate   # PowerShell: .venv\Scripts\Activate
pip install -e .[dev,dbt,airflow,quality]

# 2. Configure
cp .env.example .env
$EDITOR .env                                          # fill in Snowflake credentials

# 3. Bootstrap Snowflake (one-off)
snowsql -f sql/ddl/00_database_setup.sql
snowsql -f sql/ddl/01_raw_transactional.sql
snowsql -f sql/ddl/02_raw_reference.sql

# 4. Generate test data + ingest
monogram-generate \
    --sales 100000 --products 5000 --customers 10000 \
    --stores 20 --suppliers 10 --returns 5000 --reviews 15000 --inventory 10000
monogram-ingest-direct    --all-transactional --batch-size 10000
monogram-ingest-snowpipe  --all-reference     --batch-size 2000

# 5. Transform + test in Snowflake
cd dbt && dbt deps && dbt build --target dev

# 6. Verify
cd .. && monogram-check
```

The Airflow DAGs in [`dags/`](dags/) automate steps 3–6 on a schedule.

---

## 📐 Architecture (one-paragraph version)

**Ingestion** has two paths: (1) the direct ingester batches JSON sales/returns/reviews/inventory rows into Snowflake via parameterised `INSERT`s, and (2) the snowpipe ingester writes Parquet files to a temporary stage and `COPY INTO`s reference data (products/customers/stores/suppliers/promotions). Both share one Snowflake connection class. **Orchestration** is Airflow: `monogram_etl_pipeline` runs every 4 hours (init DDL → generate → ingest in parallel → dbt → DQ check), with a separate daily DAG for slow-moving reference data. **Transformation** is dbt: 8 staging views, 4 conformed dimensions (`dim_customer`, `dim_product`, `dim_store`, `dim_date`), 2 facts (`fact_sales`, `fact_returns`), 1 SCD2 snapshot on customers. **Quality** comes from three layers: dbt schema tests (PK uniqueness, FK relationships, accepted values), `dbt_expectations` for numeric bounds, and `sql/validation/*.sql` for ops-side freshness + referential integrity. **Monitoring** is read out from Snowflake's `ACCOUNT_USAGE.COPY_HISTORY` and `QUERY_HISTORY` via the helpers in `diagnostics/monitoring.py`.

Full picture in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). Star schema in [`docs/DATA_MODEL.md`](docs/DATA_MODEL.md).

---

## ⚡ Real-time streaming (Kafka → Snowpipe Streaming)

Alongside the batch ELT, sales stream in real time: a producer publishes events to
**Redpanda** (Kafka API), and a consumer ingests them into Snowflake with
**Snowpipe Streaming** (high-performance Python SDK), one channel per partition,
**exactly-once** via Kafka-offset tokens. dbt models the stream
(`stg_stream_sales` → `fct_sales_realtime`) and a Streamlit dashboard shows live
metrics. Full detail in [`docs/STREAMING.md`](docs/STREAMING.md).

```bash
docker compose up -d                      # Redpanda + Console (localhost:8088)
monogram-stream-produce --rate 20         # publish sale events
monogram-stream-consume --batch-size 50   # -> Snowflake STREAM_SALES (exactly-once)
streamlit run dashboard/streamlit_app.py  # insights + live stream (localhost:8501)
```

## ✅ Bloc 3 deliverables — coverage matrix

| Requirement | Where it lives |
|-------------|----------------|
| Real-time data streams | `python/monogram_etl/streaming/` (Kafka producer + consumer) → Snowpipe Streaming → `STREAM_SALES` ([`docs/STREAMING.md`](docs/STREAMING.md)) |
| ETL/ELT transformation | `dbt/models/` (staging → marts star schema + `fct_sales_realtime`) |
| Automation / orchestration | `dags/monogram_etl_dag.py`, `monogram_reference_refresh_dag.py`, `monogram_stream_monitor_dag.py` + GitHub Actions CI (`.github/workflows/ci.yml`) |
| Scheduling | 4-hourly ETL · daily reference refresh · 5-minute stream monitor |
| Monitoring & observability | stream-monitor DAG + `sql/validation/*.sql` + `diagnostics/monitoring.py` + Streamlit dashboard |
| Data quality | dbt schema tests + `dbt_expectations` + singular tests + source freshness + exactly-once |
| Error handling | Airflow retries (exp. backoff) + `dags/callbacks.py` + `utils/retry.py` + consumer offset-resume |
| Tests | `tests/{streaming,integration,data_quality}/` + `dbt build` tests |
| Insights | `dashboard/streamlit_app.py` (Streamlit on the MARTS star schema + live stream) |

---

## 🧪 Testing

```bash
pytest                 # full suite
pytest -m unit         # quick checks, no external deps
pytest -m integration  # mocks Snowflake, exercises the ingesters
pytest -m data_quality # generator output sanity
```

The dbt-side tests run as part of `dbt build` (or `dbt test`).

---

## ⚙️ Configuration

All configuration is via `.env`. See [`.env.example`](.env.example) for the full list. The most important variables are `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_PATH` (or `PRIVATE_KEY` raw PEM), and `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`.

dbt has its own [`dbt/profiles.yml.example`](dbt/profiles.yml.example) which references the same env vars — no separate Snowflake account needed.

---

## 🔐 Security

Credentials are managed through `.env` (gitignored). A past credential leak is documented in [`SECURITY_NOTE.md`](SECURITY_NOTE.md). The exposed Snowflake trial account has been deactivated; the note is left in the repo as a learning artefact.

---

## 📚 More documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — full architecture with data-flow diagrams
- [`docs/DATA_MODEL.md`](docs/DATA_MODEL.md) — ERD + star-schema rationale
- [`docs/MONITORING.md`](docs/MONITORING.md) — what's monitored, alert thresholds, dashboards
- [`docs/ERROR_HANDLING.md`](docs/ERROR_HANDLING.md) — retry / DLQ / recovery procedures
- [`dbt/README.md`](dbt/README.md) — dbt project layout, layers, tests
- [`dags/README.md`](dags/README.md) — Airflow DAG topology + deployment
- [`SECURITY_NOTE.md`](SECURITY_NOTE.md) — credential-leak disclosure
