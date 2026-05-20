{{ config(materialized='view') }}

with source as (
    select * from {{ source('monogram_raw', 'CUSTOMERS_DATA_SNOWPIPE') }}
),

renamed as (
    select
        customer_id           ::varchar(20)  as customer_id,
        first_name            ::varchar(100) as first_name,
        last_name             ::varchar(100) as last_name,
        email                 ::varchar(255) as email,
        phone                 ::varchar(50)  as phone,
        date_of_birth         ::date         as date_of_birth,
        gender                ::varchar(10)  as gender,
        address               ::varchar(500) as address,
        segment               ::varchar(50)  as segment,
        registration_date     ::date         as registration_date,
        last_purchase_date    ::date         as last_purchase_date,
        total_orders          ::integer      as total_orders,
        lifetime_value        ::number(12,2) as lifetime_value,
        preferred_channel     ::varchar(50)  as preferred_channel,
        marketing_consent     ::boolean      as marketing_consent
    from source
    where customer_id is not null
)

select * from renamed
