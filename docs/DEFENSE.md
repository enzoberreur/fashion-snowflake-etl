# Defense pack - Bloc 3 (Pipelines de Données Temps Réel)

Project: **Monogram Paris ETL** (luxury vintage fashion, Snowflake). This is your
Q&A preparation for the oral (5 min talk + 15 min Q&A). It is written in English to
match the repository documentation; if your oral is in French, ask and these can be
translated.

The jury will probe the highest-weight criteria first (Code quality 25%, Data
quality control 20%), so those sections are deepest.

---

## 30-second opening pitch

"This is a real-time sales pipeline for a luxury fashion resale business, built on
Snowflake. It has two sides. A batch ELT loads transactional and reference data and
transforms it with dbt into a star schema. A streaming path publishes sale events to
Redpanda, a Kafka-compatible broker, and ingests them into Snowflake with Snowpipe
Streaming, exactly once. Airflow orchestrates everything, three layers of data
quality checks guard the data, and a Streamlit dashboard reads both the historical
marts and the live stream. My focus was code quality and data quality: the pipeline
is a tested, installable Python package, and every load is checked and replayable."

---

## Pipeline architecture

**Q: Walk me through the pipeline.**
A: Two ingestion styles feed one warehouse. Batch: a direct ingester writes
transactional rows with parameterised INSERTs, and a Snowpipe ingester stages
Parquet and COPYs reference data. Streaming: a producer emits sale events to
Redpanda on a three-partition topic; a consumer reads them and pushes them into
Snowflake through Snowpipe Streaming. dbt then models everything into a star schema
(dimensions, fact tables, an SCD2 customer snapshot) plus a real-time fact. Airflow
schedules the batch loads, the reference refresh, and a stream monitor.

**Q: Why add streaming to a batch project?**
A: The brief is real-time pipelines, and batch alone cannot answer "what is selling
right now." The streaming path gives sub-second freshness for live sales while the
batch path keeps the heavy historical modelling. They share one warehouse and one
dbt project, so the analytics stay consistent.

**Q: Why Snowpipe Streaming instead of the Kafka Snowflake connector or micro-batches?**
A: Snowpipe Streaming writes rows directly into a table with low latency and no
intermediate files or extra connector infrastructure, and it gives me an offset
token per row that I use for exactly-once. A connector would add a component to run;
micro-batch COPY would add seconds of latency and file management. For a low-latency
single-table sink, streaming was the cleanest fit.

**Q: Why Redpanda rather than Kafka?**
A: It speaks the Kafka API, so the code is portable, but it is a single binary with
no ZooKeeper, which makes it realistic to run for a demo while staying
production-credible. If the company already ran Kafka, the same producer and consumer
would work unchanged.

## Code quality

**Q: How is the code organised, and why does that help quality?**
A: It is an installable package, `monogram_etl`, with clear modules: config (a single
Snowflake connection class so authentication has one code path), ingesters,
generators, streaming, diagnostics, and utils. Console scripts expose the entry
points. There is one way to do each thing, which keeps the surface small and
testable.

**Q: How do you test it, and what is covered?**
A: pytest, split into unit, integration, and data_quality markers, run in CI with
coverage on every push. Unit tests cover the pure logic (event building, config,
retry); integration tests mock Snowflake and exercise the ingesters; data_quality
tests sanity-check generated data. The dbt side adds its own tests via `dbt build`.

**Q: How do you authenticate to Snowflake safely?**
A: Key-pair authentication, never a password in code. The private key path and
passphrase come from environment variables (`.env`, gitignored). There is a single
connection class so credentials are handled in one place.

## Data quality control

**Q: How do you guarantee data quality?**
A: Three layers. dbt schema tests enforce primary key uniqueness, foreign key
relationships, and accepted values. `dbt_expectations` enforces numeric and
distribution bounds. Singular SQL tests and `sql/validation/*.sql` check operational
concerns: source freshness, referential integrity, and exactly-once. If any critical
test fails, `dbt build` fails, so bad data does not reach the marts.

**Q: How do you achieve exactly-once on the stream?**
A: One Snowpipe Streaming channel per Kafka partition, and the Kafka offset is used
as the Snowpipe offset token. On restart the consumer resumes from the last
committed offset and Snowpipe rejects any token it has already seen, so a crash or a
replay cannot create duplicates. I verify this with a dedicated test that asserts no
duplicate offsets in the target table.

**Q: How do you handle late, malformed, or duplicate events?**
A: Duplicates are handled by the offset-token mechanism. Malformed events are caught
at the consumer boundary; the production extension is a dead-letter topic for events
that fail validation. Freshness is monitored, so a stalled stream raises an alert
rather than silently going stale.

## Automation and error handling

**Q: What is automated, and on what schedule?**
A: Three Airflow DAGs: the main ETL every four hours, a daily reference refresh, and
a stream monitor every five minutes. GitHub Actions runs lint and tests on every
push and pull request.

**Q: What happens when a load fails?**
A: Airflow retries with exponential backoff and runs failure callbacks. The streaming
consumer resumes from its committed offset, so it never loses or double-counts
events. The retry helper in `utils/retry.py` wraps the transient Snowflake calls.

## Monitoring and operations

**Q: How do you know the pipeline is healthy in production?**
A: The stream-monitor DAG checks freshness, exactly-once, and throughput every five
minutes. `diagnostics/monitoring.py` reads Snowflake's own ACCOUNT_USAGE views
(COPY and QUERY history) for load history and cost. The Streamlit dashboard has a
live tab showing latency, partition lag, and freshness.

**Q: A subtle one: why SYSDATE() and not CURRENT_TIMESTAMP() in your checks?**
A: The ingestion timestamp is stored in UTC, and CURRENT_TIMESTAMP returns the
session timezone, which produced negative freshness values. SYSDATE is UTC, so the
freshness math is correct. It was a real bug I found and fixed.

## Reflection

**Q: What was the hardest part?**
A: Getting exactly-once genuinely right. My first attempt appended rows without
offset tokens and nothing committed. Understanding that the offset token is what
makes Snowpipe Streaming idempotent, and mapping it to the Kafka offset per
partition, was the key insight.

**Q: What would you do differently or next?**
A: Add a schema registry so event contracts are versioned, source the stream from a
real OLTP system via change data capture instead of a generator, and add a
dead-letter topic for poison messages. Those turn it from a strong demo into a
production service.

**Q: I see a SECURITY_NOTE about a leaked credential. Explain.**
A: An early Snowflake trial key was committed by mistake. I rotated it, the trial
account is dead, and I kept the note in the repo as an honest disclosure and a
learning artifact. The current design keeps all secrets in a gitignored `.env`.
