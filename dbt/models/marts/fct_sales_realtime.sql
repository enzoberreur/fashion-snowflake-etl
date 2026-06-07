{{
    config(
        materialized='table',
        unique_key='event_sk',
        cluster_by=['event_ts']
    )
}}

-- Real-time sales fact built from the streamed events, conformed to the same
-- surrogate keys as fact_sales so it joins dim_customer / dim_product / dim_store /
-- dim_date. Carries the per-event streaming latency for monitoring.

with stream as (
    select * from {{ ref('stg_stream_sales') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.event_id']) }}      as event_sk,
        s.event_id,
        s.sale_id,
        to_number(to_char(s.sale_date, 'YYYYMMDD'))                 as date_sk,
        {{ dbt_utils.generate_surrogate_key(['s.customer_id']) }}   as customer_sk,
        {{ dbt_utils.generate_surrogate_key(['s.product_id']) }}    as product_sk,
        {{ dbt_utils.generate_surrogate_key(['s.store_id']) }}      as store_sk,
        s.event_ts,
        s.sale_date,
        s.customer_id,
        s.product_id,
        s.store_id,
        s.channel,
        s.country,
        s.quantity,
        s.unit_price,
        s.total_amount                                             as gross_amount,
        s.kafka_partition,
        s.kafka_offset,
        s.ingested_at,
        s.latency_seconds,
        current_timestamp()                                        as dbt_loaded_at
    from stream s
)

select * from joined
