# Demo Script - Bloc 3 (3-5 minutes)

Required flow: **Ingestion → Transformation → Loading → Monitoring / Data Quality**.
Record at 1920x1080. Pre-flight (off camera): `docker compose up -d`, `.env` set,
`STREAM_SALES` truncated, one batch already loaded so MARTS is populated, and one
training-free `dbt build` done.

> Pre-flight: `docker compose ps` (Redpanda healthy) · open Redpanda Console
> `http://localhost:8088` · `streamlit run dashboard/streamlit_app.py`.

---

### 0:00-0:30 - Intro & architecture
"Monogram Paris is a luxury-fashion data platform. This is its real-time pipeline:
sales stream through Kafka into Snowflake via Snowpipe Streaming, dbt builds a star
schema, and everything is monitored for data quality." Show the architecture
diagram (`docs/STREAMING.md` / the deck).

### 0:30-1:30 - Ingestion (real-time)
- Terminal 1: `monogram-stream-produce --rate 20` - events start flowing.
- Open **Redpanda Console** (`localhost:8088`) → topic `monogram.sales.stream` →
  show live messages landing across 3 partitions.
- Terminal 2: `monogram-stream-consume --batch-size 50` - watch it append to
  Snowflake; point out "1 channel per partition, Kafka offset = offset token".

### 1:30-2:15 - Transformation (dbt)
- `cd dbt && dbt build --select stg_stream_sales+ fct_sales_realtime`
- Show the layers: `INGEST.STREAM_SALES` (raw) → `STAGING.STG_STREAM_SALES` (view)
  → `MARTS.FCT_SALES_REALTIME` (conformed to dim_customer/product/store/date), and
  the batch `fact_sales` star schema alongside it.

### 2:15-3:00 - Loading & insights (dashboard)
- Open **Streamlit** (`localhost:8501`).
- **Business insights** tab: revenue (~€250M), by channel/country, monthly trend,
  top brands, customer value tiers - all from the MARTS star schema.
- **Real-time stream** tab: events count, end-to-end latency, partitions,
  freshness, and the latest streamed sales updating live.

### 3:00-4:00 - Monitoring & data quality
- `dbt test --select stg_stream_sales` and `dbt source freshness` - show the
  exactly-once combination test and freshness pass.
- Show `sql/validation/assert_stream_*.sql` results (freshness / exactly-once /
  throughput) and the **Airflow** `monogram_stream_monitor` DAG graph.
- "Same checks run every 5 minutes; a stuck or double-writing consumer fails loud."

### 4:00-4:30 - Wrap-up
"Ingestion → transformation → loading → monitoring, in real time and on a tested,
orchestrated, quality-gated pipeline. Code, IaC and this demo are in the repo."
Show the repo tree (`/dags /sql /python /tests /dbt /dashboard`).

---

## Shot checklist
- [ ] Producer streaming + Redpanda Console messages (3 partitions)
- [ ] Consumer landing into Snowflake (exactly-once note)
- [ ] dbt build: staging + real-time fact + star schema
- [ ] Streamlit: business insights + live stream tab
- [ ] dbt tests + source freshness + SQL validations
- [ ] Airflow stream-monitor DAG graph
