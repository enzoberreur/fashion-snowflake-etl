-- assert_stream_exactly_once.sql
-- Exactly-once check for the streaming path: each Kafka (partition, offset) must
-- land at most once. Returns the number of duplicated groups; 0 = healthy.
-- Used by the Airflow stream-monitor DAG (any value > 0 fails the run).

USE ROLE INGEST;
USE DATABASE INGEST;
USE SCHEMA INGEST;

select
    'stream_duplicate_offsets' as check_name,
    count(*)                   as offending_groups
from (
    select KAFKA_PARTITION, KAFKA_OFFSET
    from STREAM_SALES
    group by 1, 2
    having count(*) > 1
);
