{{ config(materialized='view') }}

with source as (
    select * from {{ source('monogram_raw', 'INVENTORY_DATA') }}
),

renamed as (
    select
        inventory_id         ::varchar(10) as inventory_id,
        product_id           ::varchar(10) as product_id,
        store_id             ::varchar(10) as store_id,
        current_stock        ::integer     as current_stock,
        reserved_stock       ::integer     as reserved_stock,
        reorder_level        ::integer     as reorder_level,
        max_stock_level      ::integer     as max_stock_level,
        last_restocked       ::date        as last_restocked,
        next_delivery_date   ::date        as next_delivery_date,
        warehouse_location   ::varchar(50) as warehouse_location,
        current_stock - reserved_stock     as available_stock,
        created_at           ::timestamp_ntz as ingested_at
    from source
    where inventory_id is not null
      and current_stock >= 0
      and reserved_stock >= 0
)

select * from renamed
