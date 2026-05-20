{{
    config(
        materialized='table',
        unique_key='product_sk'
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
),

suppliers as (
    select * from {{ ref('stg_suppliers') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['p.product_id']) }}             as product_sk,
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.material,
        p.color,
        p.sku,
        p.price,
        p.cost,
        p.gross_margin,
        round(p.gross_margin / nullifzero(p.price) * 100, 2)                 as margin_pct,
        p.weight_kg,
        p.dimensions_cm,
        p.supplier_id,
        s.supplier_name,
        s.specialty                                                          as supplier_specialty,
        s.quality_rating                                                     as supplier_quality_rating,
        s.lead_time_days                                                     as supplier_lead_time_days,
        p.created_date,
        p.last_updated,
        p.is_active,
        current_timestamp()                                                  as dbt_loaded_at
    from products p
    left join suppliers s on p.supplier_id = s.supplier_id
)

select * from enriched
