{{ config(materialized='view') }}

-- Cleaned real-time sales events landed by Snowpipe Streaming into INGEST.STREAM_SALES.
-- latency_seconds = event time (sale) to land time, the end-to-end streaming latency.

with source as (
    select * from {{ source('monogram_raw', 'STREAM_SALES') }}
),

renamed as (
    select
        event_id        ::varchar(36)    as event_id,
        sale_id         ::varchar(20)    as sale_id,
        event_ts        ::timestamp_ntz  as event_ts,
        sale_date       ::date           as sale_date,
        customer_id     ::varchar(20)    as customer_id,
        product_id      ::varchar(20)    as product_id,
        product_name    ::varchar(200)   as product_name,
        quantity        ::integer        as quantity,
        unit_price      ::number(12,2)   as unit_price,
        total_amount    ::number(14,2)   as total_amount,
        channel         ::varchar(30)    as channel,
        store_id        ::varchar(20)    as store_id,
        country         ::varchar(50)    as country,
        kafka_partition ::integer        as kafka_partition,
        kafka_offset    ::number(20,0)   as kafka_offset,
        ingested_at     ::timestamp_ntz  as ingested_at,
        datediff('millisecond', event_ts, ingested_at) / 1000.0 as latency_seconds
    from source
    where event_id is not null
      and customer_id is not null
      and product_id is not null
      and quantity > 0
      and total_amount >= 0
)

select * from renamed
