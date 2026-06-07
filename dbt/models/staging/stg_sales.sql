{{ config(materialized='view') }}

with source as (
    select * from {{ source('monogram_raw', 'SALES_DATA') }}
),

renamed as (
    select
        sale_id            ::varchar(10)  as sale_id,
        sale_date          ::date         as sale_date,
        customer_id        ::varchar(10)  as customer_id,
        product_id         ::varchar(10)  as product_id,
        product_name       ::varchar(100) as product_name,
        quantity           ::integer      as quantity,
        unit_price         ::number(10,2) as unit_price,
        total_amount       ::number(10,2) as total_amount,
        channel            ::varchar(20)  as channel,
        store_id           ::varchar(10)  as store_id,
        country            ::varchar(50)  as country,
        created_at         ::timestamp_ntz as ingested_at
    from source
    where sale_id is not null
      and customer_id is not null
      and product_id is not null
      and quantity > 0
      and total_amount >= 0
)

select * from renamed
