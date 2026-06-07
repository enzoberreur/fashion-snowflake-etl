-- Exactly-once guarantee for the streaming path: no Kafka (partition, offset)
-- should appear more than once in the staged stream. Returns offending rows
-- (which fails the test) if Snowpipe Streaming ever double-committed.

select
    kafka_partition,
    kafka_offset,
    count(*) as occurrences
from {{ ref('stg_stream_sales') }}
group by 1, 2
having count(*) > 1
