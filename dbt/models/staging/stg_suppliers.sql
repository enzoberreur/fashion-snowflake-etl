{{ config(materialized='view') }}

with source as (
    select * from {{ source('monogram_raw', 'SUPPLIERS_DATA_SNOWPIPE') }}
),

renamed as (
    select
        supplier_id       ::varchar(20)  as supplier_id,
        name              ::varchar(255) as supplier_name,
        contact_person    ::varchar(255) as contact_person,
        email             ::varchar(255) as email,
        phone             ::varchar(50)  as phone,
        address           ::varchar(500) as address,
        specialty         ::varchar(255) as specialty,
        lead_time_days    ::integer      as lead_time_days,
        minimum_order     ::number(10,2) as minimum_order,
        payment_terms     ::varchar(100) as payment_terms,
        quality_rating    ::number(3,2)  as quality_rating,
        established_date  ::date         as established_date,
        is_active         ::boolean      as is_active
    from source
    where supplier_id is not null
)

select * from renamed
