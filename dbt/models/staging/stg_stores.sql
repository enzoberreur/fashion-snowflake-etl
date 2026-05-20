{{ config(materialized='view') }}

with source as (
    select * from {{ source('monogram_raw', 'STORES_DATA_SNOWPIPE') }}
),

renamed as (
    select
        store_id         ::varchar(20)  as store_id,
        store_name       ::varchar(255) as store_name,
        manager_name     ::varchar(255) as manager_name,
        address          ::varchar(500) as address,
        city             ::varchar(100) as city,
        country          ::varchar(100) as country,
        phone            ::varchar(50)  as phone,
        email            ::varchar(255) as email,
        opening_date     ::date         as opening_date,
        store_size_sqm   ::number(10,2) as store_size_sqm,
        is_active        ::boolean      as is_active
    from source
    where store_id is not null
)

select * from renamed
