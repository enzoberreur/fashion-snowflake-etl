# Evaluation criteria coverage - Bloc 3 (Pipelines de Données Temps Réel)

This table maps every jury grading criterion to exactly where it is demonstrated,
across the three graded artifacts: the slide deck, this repository, and the Loom
demo. Use it during the defense so each point is easy to verify.

| Criterion | Weight | Where it is proven |
|-----------|:------:|--------------------|
| Code quality | 25% | Installable package `python/monogram_etl/` (single Snowflake auth path in `config/`, `utils/retry.py` backoff, typed config dataclasses for streaming). `tests/` (unit / integration / data_quality). CI runs `pytest` with coverage (`.github/workflows/ci.yml`). `ruff` linting + `.pre-commit-config.yaml`. |
| Data quality control | 20% | Three layers: dbt schema tests (PK/FK/accepted values) + `dbt_expectations` (numeric bounds) + singular tests; dbt source freshness; `sql/validation/*.sql` (freshness, referential integrity, exactly-once). Exactly-once delivery enforced via Kafka offset tokens (one channel per partition). |
| Pipeline architecture | 20% | `docs/ARCHITECTURE.md`, `docs/STREAMING.md`, `docs/architecture.png`. Two batch ingestion paths (direct SQL INSERT + Snowpipe Parquet/COPY) plus a real-time path: producer -> Redpanda (Kafka API) -> consumer -> Snowpipe Streaming -> Snowflake, modelled by dbt into a star schema + a real-time fact. |
| Automation | 15% | `dags/` Airflow DAGs: `monogram_etl_dag` (every 4h), `monogram_reference_refresh_dag` (daily), `monogram_stream_monitor_dag` (every 5 min). GitHub Actions CI on push/PR. |
| Monitoring | 10% | Stream-monitor DAG (freshness + exactly-once + throughput), `diagnostics/monitoring.py` (Snowflake `ACCOUNT_USAGE` COPY/QUERY history), Streamlit dashboard live-stream tab (latency, partitions, freshness). |
| Functional pipeline | 5% | Verified end to end: `monogram-stream-produce` -> Redpanda (3 partitions) -> `monogram-stream-consume` -> `STREAM_SALES` (exactly-once) -> `dbt build` -> Streamlit. **Loom**: ingestion -> transformation -> loading -> monitoring. |
| Presentation | 5% | `deliverables/presentation.pptx` + 5-minute oral. See `docs/DEFENSE.md` for the Q&A. |

**Reading tip for the jury:** the highest weights are Code quality (25%) and Data
quality control (20%). Code quality is shown by the package layout + tests + CI
coverage; data quality is the three-layer testing strategy plus exactly-once
delivery, all runnable via `dbt build` and the `sql/validation` scripts.
