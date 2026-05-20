{{
    config(
        materialized='table',
        unique_key='customer_sk'
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

sales_agg as (
    select
        customer_id,
        count(*)                                   as observed_orders,
        sum(total_amount)                          as observed_revenue,
        min(sale_date)                             as first_order_date,
        max(sale_date)                             as last_order_date,
        count(distinct date_trunc('month', sale_date)) as active_months
    from {{ ref('stg_sales') }}
    group by 1
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }}            as customer_sk,
        c.customer_id,
        c.first_name,
        c.last_name,
        c.first_name || ' ' || c.last_name                                   as full_name,
        c.email,
        c.phone,
        c.date_of_birth,
        datediff('year', c.date_of_birth, current_date())                    as age,
        c.gender,
        c.segment,
        c.preferred_channel,
        c.marketing_consent,
        c.registration_date,
        c.last_purchase_date,
        c.total_orders                                                       as crm_orders,
        c.lifetime_value                                                     as crm_lifetime_value,
        coalesce(s.observed_orders,   0)                                     as observed_orders,
        coalesce(s.observed_revenue, 0)                                      as observed_revenue,
        s.first_order_date,
        s.last_order_date,
        s.active_months,
        case
            when s.observed_revenue >= 5000 then 'VIP'
            when s.observed_revenue >= 1000 then 'High Value'
            when s.observed_revenue >= 100  then 'Regular'
            when s.observed_orders   >= 1   then 'Occasional'
            else 'Inactive'
        end                                                                  as value_tier,
        current_timestamp()                                                  as dbt_loaded_at
    from customers c
    left join sales_agg s using (customer_id)
)

select * from enriched
