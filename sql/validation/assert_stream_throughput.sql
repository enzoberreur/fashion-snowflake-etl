-- assert_stream_throughput.sql
-- Throughput + latency snapshot for the streaming path over the last 15 minutes.
-- Informational (surfaced by the stream-monitor DAG and the demo); does not fail.

USE ROLE INGEST;
USE DATABASE INGEST;
USE SCHEMA INGEST;

select
    count(*)                                                                   as events_last_15min,
    round(avg(datediff('millisecond', EVENT_TS, INGESTED_AT)) / 1000.0, 3)     as avg_latency_seconds,
    round(max(datediff('millisecond', EVENT_TS, INGESTED_AT)) / 1000.0, 3)     as max_latency_seconds,
    count(distinct KAFKA_PARTITION)                                            as active_partitions
from STREAM_SALES
where INGESTED_AT >= dateadd('minute', -15, sysdate());
