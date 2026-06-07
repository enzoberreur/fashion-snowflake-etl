-- assert_stream_freshness.sql
-- Flags the real-time stream if the most recently landed event is older than the
-- SLA. SLA: 5 minutes (the consumer should be ingesting continuously).
-- Used by the Airflow stream-monitor DAG.

USE ROLE INGEST;
USE DATABASE INGEST;
USE SCHEMA INGEST;

with freshness as (
    select
        'STREAM_SALES'   as table_name,
        max(INGESTED_AT) as last_loaded_at,
        5                as sla_minutes,
        count(*)         as row_count
    from STREAM_SALES
)
select
    table_name,
    last_loaded_at,
    sla_minutes,
    timestampdiff('minute', last_loaded_at, sysdate()) as minutes_since_last_load,
    row_count,
    case
        when last_loaded_at is null then 'EMPTY'
        when timestampdiff('minute', last_loaded_at, sysdate()) > sla_minutes then 'STALE'
        else 'FRESH'
    end as freshness_status
from freshness;
