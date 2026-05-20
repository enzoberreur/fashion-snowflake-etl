{{
    config(
        materialized='table',
        unique_key='return_sk',
        cluster_by=['return_date']
    )
}}

with returns as (
    select * from {{ ref('stg_returns') }}
),

sales as (
    select sale_id, sale_date, store_id, channel, total_amount as original_sale_amount
    from {{ ref('stg_sales') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['r.return_id']) }}                              as return_sk,
    r.return_id,
    r.sale_id,
    {{ dbt_utils.generate_surrogate_key(['r.sale_id']) }}                                as sale_sk,
    to_number(to_char(r.return_date, 'YYYYMMDD'))                                        as date_sk,
    {{ dbt_utils.generate_surrogate_key(['r.customer_id']) }}                            as customer_sk,
    {{ dbt_utils.generate_surrogate_key(['r.product_id']) }}                             as product_sk,
    {{ dbt_utils.generate_surrogate_key(['s.store_id']) }}                               as store_sk,
    r.return_date,
    s.sale_date,
    datediff('day', s.sale_date, r.return_date)                                          as days_to_return,
    r.customer_id,
    r.product_id,
    s.store_id,
    s.channel,
    r.return_reason,
    r.item_condition,
    r.status,
    r.refund_method,
    r.refund_amount,
    s.original_sale_amount,
    round(r.refund_amount / nullifzero(s.original_sale_amount) * 100, 2)                 as refund_pct,
    r.notes,
    r.ingested_at,
    current_timestamp()                                                                  as dbt_loaded_at
from returns r
left join sales s on r.sale_id = s.sale_id
