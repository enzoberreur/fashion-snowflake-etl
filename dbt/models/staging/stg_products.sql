{{ config(materialized='view') }}

with source as (
    select * from {{ source('monogram_raw', 'PRODUCTS_DATA_SNOWPIPE') }}
),

renamed as (
    select
        product_id      ::varchar(20)  as product_id,
        name            ::varchar(255) as product_name,
        category        ::varchar(100) as category,
        subcategory     ::varchar(100) as subcategory,
        brand           ::varchar(100) as brand,
        material        ::varchar(100) as material,
        color           ::varchar(50)  as color,
        price           ::number(10,2) as price,
        cost            ::number(10,2) as cost,
        price - cost                   as gross_margin,
        weight_kg       ::number(8,2)  as weight_kg,
        dimensions_cm   ::varchar(50)  as dimensions_cm,
        supplier_id     ::varchar(20)  as supplier_id,
        created_date    ::date         as created_date,
        last_updated    ::date         as last_updated,
        is_active       ::boolean      as is_active,
        sku             ::varchar(50)  as sku
    from source
    where product_id is not null
      and price > 0
)

select * from renamed
