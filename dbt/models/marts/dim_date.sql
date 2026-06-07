{{
    config(
        materialized='table',
        unique_key='date_sk'
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('start_date') ~ "' as date)",
        end_date="cast('" ~ var('end_date') ~ "' as date)"
    ) }}
)

select
    to_number(to_char(date_day, 'YYYYMMDD'))            as date_sk,
    date_day                                            as full_date,
    extract(year    from date_day)                      as year,
    extract(quarter from date_day)                      as quarter,
    extract(month   from date_day)                      as month,
    to_char(date_day, 'MMMM')                           as month_name,
    extract(week    from date_day)                      as week_of_year,
    extract(day     from date_day)                      as day_of_month,
    extract(dayofweek from date_day)                    as day_of_week,
    to_char(date_day, 'Dy')                             as day_name,
    case when extract(dayofweek from date_day) in (0, 6) then true else false end as is_weekend,
    date_trunc('quarter', date_day)                     as quarter_start_date,
    date_trunc('year', date_day)                        as year_start_date,
    current_timestamp()                                 as dbt_loaded_at
from date_spine
