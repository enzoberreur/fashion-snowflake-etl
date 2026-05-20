{{
    config(
        materialized='table',
        unique_key='sale_sk',
        cluster_by=['sale_date']
    )
}}

with sales as (
    select * from {{ ref('stg_sales') }}
),

returns_agg as (
    select
        sale_id,
        count(*)              as return_count,
        sum(refund_amount)    as total_refund_amount
    from {{ ref('stg_returns') }}
    group by 1
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.sale_id']) }}                                as sale_sk,
        s.sale_id,
        to_number(to_char(s.sale_date, 'YYYYMMDD'))                                          as date_sk,
        {{ dbt_utils.generate_surrogate_key(['s.customer_id']) }}                            as customer_sk,
        {{ dbt_utils.generate_surrogate_key(['s.product_id']) }}                             as product_sk,
        {{ dbt_utils.generate_surrogate_key(['s.store_id']) }}                               as store_sk,
        s.sale_date,
        s.customer_id,
        s.product_id,
        s.store_id,
        s.channel,
        s.country,
        s.quantity,
        s.unit_price,
        s.total_amount                                                                       as gross_amount,
        coalesce(r.return_count, 0)                                                          as return_count,
        coalesce(r.total_refund_amount, 0)                                                   as refund_amount,
        s.total_amount - coalesce(r.total_refund_amount, 0)                                  as net_amount,
        case when coalesce(r.return_count, 0) > 0 then true else false end                   as has_return,
        s.ingested_at,
        current_timestamp()                                                                  as dbt_loaded_at
    from sales s
    left join returns_agg r on r.sale_id = s.sale_id
)

select * from joined
