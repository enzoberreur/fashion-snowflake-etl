{{ config(materialized='view') }}

with source as (
    select * from {{ source('monogram_raw', 'REVIEWS_DATA') }}
),

renamed as (
    select
        review_id          ::varchar(10)   as review_id,
        product_id         ::varchar(10)   as product_id,
        customer_id        ::varchar(10)   as customer_id,
        rating             ::integer       as rating,
        title              ::varchar(100)  as title,
        comment            ::varchar(1000) as comment,
        review_date        ::date          as review_date,
        verified_purchase  ::boolean       as is_verified_purchase,
        helpful_votes      ::integer       as helpful_votes,
        status             ::varchar(20)   as status,
        created_at         ::timestamp_ntz as ingested_at
    from source
    where review_id is not null
      and rating between 1 and 5
)

select * from renamed
