{{ config(materialized='view') }}

with source as (
    select * from {{ source('monogram_raw', 'RETURNS_DATA') }}
),

renamed as (
    select
        return_id       ::varchar(10)  as return_id,
        sale_id         ::varchar(10)  as sale_id,
        customer_id     ::varchar(10)  as customer_id,
        product_id      ::varchar(10)  as product_id,
        return_date     ::date         as return_date,
        reason          ::varchar(50)  as return_reason,
        condition       ::varchar(20)  as item_condition,
        refund_amount   ::number(10,2) as refund_amount,
        refund_method   ::varchar(30)  as refund_method,
        processed_by    ::varchar(100) as processed_by,
        status          ::varchar(20)  as status,
        notes           ::varchar(500) as notes,
        created_at      ::timestamp_ntz as ingested_at
    from source
    where return_id is not null
      and sale_id   is not null
      and refund_amount >= 0
)

select * from renamed
