{{
    config(
        materialized='table',
        unique_key='store_sk'
    )
}}

with stores as (
    select * from {{ ref('stg_stores') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['store_id']) }}             as store_sk,
    store_id,
    store_name,
    manager_name,
    address,
    city,
    country,
    case country
        when 'France'         then 'Europe'
        when 'Italy'          then 'Europe'
        when 'United Kingdom' then 'Europe'
        when 'Spain'          then 'Europe'
        when 'Germany'        then 'Europe'
        when 'United States'  then 'Americas'
        when 'Japan'          then 'Asia-Pacific'
        else 'Other'
    end                                                              as region,
    phone,
    email,
    opening_date,
    datediff('year', opening_date, current_date())                   as years_open,
    store_size_sqm,
    is_active,
    current_timestamp()                                              as dbt_loaded_at
from stores
