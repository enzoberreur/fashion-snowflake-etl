# Loom Demo Script - Bloc 3 (Real-time Data Pipelines)

**Goal:** in one take, show everything the jury grades: the real-time architecture,
the code, the automation, the data-quality controls, the monitoring, and proof the
pipeline runs end to end.

**Required flow:** Ingestion -> Transformation -> Loading -> Monitoring / Data Quality.

**Target length:** 4 to 5 minutes. Record at 1920x1080, browser zoom ~110%.

**Project:** Monogram Paris luxury-fashion data platform. Sales stream through
Kafka/Redpanda into Snowflake via Snowpipe Streaming; dbt builds a Kimball star
schema; everything is tested and monitored.

---

## Pre-flight (off camera, so the recording stays smooth)

1. `docker compose up -d` (Redpanda + console + dashboard); `docker compose ps`
   shows Redpanda healthy.
2. `.env` set with Snowflake creds; package installed: `pip install -e .`.
3. Seed state so screens are populated: truncate `STREAM_SALES`, run one batch so
   `MARTS` is filled, and do one `dbt build` so models exist.
4. Tabs ready: Redpanda Console `http://localhost:8088`, Streamlit
   `http://localhost:8501`, Airflow `http://localhost:8080`, and the GitHub
   Actions tab on the repo.
5. Two terminals open at the repo root (producer and consumer).

---

## 0:00 - 0:30 - Intro and architecture  *(criterion: Pipeline architecture, 20%)*

**SAY:** "This is Monogram Paris's real-time pipeline. Sales events stream through
Kafka into Snowflake using Snowpipe Streaming, dbt transforms them into a star
schema, and the whole flow is tested, orchestrated and monitored. The design
supports three ingestion paths - direct insert, Parquet plus COPY, and the
real-time Kafka stream - all converging on the same conformed model."

**SHOW:** the architecture diagram (`docs/architecture.png` or `docs/STREAMING.md`).

---

## 0:30 - 1:00 - The code  *(criterion: Code quality, 25%)*

**SAY:** "The pipeline is an installable, typed Python package, not loose scripts."

**SHOW + CMD:**
- `python/monogram_etl/`: point out `config/snowflake.py` (one context-managed auth
  path), the jittered-backoff retry decorator, and the six CLI entry points in
  `pyproject.toml` (`monogram-generate`, `-ingest-direct`, `-ingest-snowpipe`,
  `-stream-produce`, `-stream-consume`, `-check`).
- Run the tests on camera: `pytest -q` (unit, integration, streaming,
  data_quality), then open the green run on the GitHub **Actions** tab (ruff +
  pytest + coverage gate, mirrored by pre-commit).

---

## 1:00 - 1:45 - Ingestion, real time  *(criterion: Architecture 20% + Functional 5%)*

**CMD (terminal 1):** `monogram-stream-produce --rate 20`  -> events start flowing.

**SHOW:** Redpanda Console `http://localhost:8088` -> topic `monogram.sales.stream`
-> live messages landing across 3 partitions.

**CMD (terminal 2):** `monogram-stream-consume --batch-size 50`  -> appends to
Snowflake.

**SAY:** "One channel per partition, and the Kafka offset is the Snowpipe offset
token, so ingestion is exactly-once by construction."

---

## 1:45 - 2:30 - Transformation with dbt  *(criterion: Code quality 25%)*

**CMD:** `cd dbt && dbt build --select stg_stream_sales+ fct_sales_realtime`

**SHOW:** the layers move from raw to mart:
`INGEST.STREAM_SALES` (raw) -> `STAGING.STG_STREAM_SALES` (view) ->
`MARTS.FCT_SALES_REALTIME` (conformed to dim_customer / product / store / date),
with the batch `fact_sales` star schema alongside it.

**SAY:** "Real-time and batch facts share the same conformed dimensions, so the
numbers reconcile."

---

## 2:30 - 3:00 - Loading and insights  *(criterion: Functional 5% + Architecture 20%)*

**SHOW:** Streamlit `http://localhost:8501`.
- **Business insights** tab: total revenue (~250M EUR), by channel and country,
  monthly trend, top brands, customer value tiers - all from the MARTS star schema.
- **Real-time stream** tab: event count, end-to-end latency, partitions, freshness,
  and the latest streamed sales updating live.

---

## 3:00 - 3:50 - Data quality control  *(criterion: Data quality control, 20%)*

**SAY:** "Quality is enforced in three layers, and a failure stops the pipeline
loud."

**CMD + SHOW:**
- `dbt test --select stg_stream_sales` -> the not_null / unique / relationships /
  accepted_values plus `dbt_expectations` numeric bounds (about 110 assertions
  across sources, staging and marts), and the singular business tests: amount
  consistency, no future-dated sales, no duplicate stream offsets.
- `dbt source freshness` -> source SLAs pass.
- `sql/validation/assert_stream_freshness.sql`, `assert_stream_exactly_once.sql`,
  `assert_stream_throughput.sql` -> the same checks the orchestrator runs.

**SAY:** "Exactly-once is enforced structurally by the offset token and then
verified by an assertion, so a double-writing consumer fails the run."

---

## 3:50 - 4:30 - Automation and monitoring  *(criteria: Automation 15% + Monitoring 10%)*

**SHOW:** Airflow `http://localhost:8080` -> three DAGs on three clocks:
- `monogram_etl_dag` (every 4 hours): TaskGroups, parallel ingest, exponential
  backoff, SLAs, execution timeouts, `max_active_runs=1`, failure/success callbacks.
- `monogram_reference_refresh_dag` (daily): dimension refresh.
- `monogram_stream_monitor_dag` (every 5 minutes): runs the SQL validations above
  and a `data_quality_task` that fails on a bad result.

**SAY:** "Monitoring reads Snowflake's own `ACCOUNT_USAGE.COPY_HISTORY` and
`QUERY_HISTORY` with per-caller query tags." Show `monogram-check` output or
`docs/MONITORING.md` (coverage matrix, SLOs, alert routing, runbook).

---

## 4:30 - 5:00 - Wrap  *(criterion: Presentation and Q&A, 5%)*

**SAY:** "Ingestion, transformation, loading, and monitoring - in real time, on a
tested, orchestrated, quality-gated pipeline with exactly-once delivery. Code, IaC
and this demo are all in the repo." Show the tree (`/dags /sql /python /tests /dbt
/dashboard`) and mention `docs/DEFENSE.md` (Q&A pack) and `docs/EVALUATION.md`
(criterion-to-evidence map).

Stop the stack after recording: `docker compose down`.

---

## Rubric coverage map

| Criterion | Weight | Shown at |
|---|---|---|
| Pipeline architecture | 20% | 0:00, 1:00, 2:30 |
| Code quality | 25% | 0:30, 1:45 |
| Automation | 15% | 3:50 |
| Data quality control | 20% | 3:00 |
| Monitoring and observability | 10% | 3:50 |
| Functional pipeline | 5% | 1:00, 2:30 |
| Presentation and Q&A | 5% | 0:00 + 4:30 |

## Shot checklist
- [ ] Architecture diagram
- [ ] Typed package + CLI entry points + `pytest` + green CI
- [ ] Producer streaming + Redpanda Console (3 partitions)
- [ ] Consumer landing into Snowflake (exactly-once note)
- [ ] dbt build: staging + real-time fact + star schema
- [ ] Streamlit: business insights + live stream tab
- [ ] dbt tests + source freshness + SQL validations
- [ ] Airflow: the three DAGs (graph + schedule)
- [ ] Monitoring via ACCOUNT_USAGE / docs/MONITORING.md
- [ ] Repo tree on the wrap

## Paste the Loom URL into
- `README.md` (top, a "Demo video" line)
- `docs/EVALUATION.md`
- The closing slide of `deliverables/presentation.pptx`
