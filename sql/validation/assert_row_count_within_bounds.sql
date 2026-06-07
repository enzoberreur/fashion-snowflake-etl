-- assert_row_count_within_bounds.sql
-- Row-count sanity check vs. expected ranges from the data generator config.
-- Drift outside these bounds indicates a load failure or a generator change.

USE ROLE INGEST;
USE DATABASE INGEST;
USE SCHEMA INGEST;

with expected_counts as (
    select 'SALES_DATA'                  as table_name, 50000  as min_rows, 1000000 as max_rows
    union all select 'RETURNS_DATA',                    1000,                100000
    union all select 'REVIEWS_DATA',                    5000,                200000
    union all select 'INVENTORY_DATA',                  1000,                100000
    union all select 'PRODUCTS_DATA_SNOWPIPE',          100,                 50000
    union all select 'CUSTOMERS_DATA_SNOWPIPE',         1000,                500000
    union all select 'STORES_DATA_SNOWPIPE',            5,                   500
    union all select 'SUPPLIERS_DATA_SNOWPIPE',         5,                   1000
    union all select 'PROMOTIONS_DATA_SNOWPIPE',        10,                  2000
),

actual_counts as (
    select 'SALES_DATA'              as table_name, count(*) as actual_rows from SALES_DATA
    union all select 'RETURNS_DATA',                count(*)               from RETURNS_DATA
    union all select 'REVIEWS_DATA',                count(*)               from REVIEWS_DATA
    union all select 'INVENTORY_DATA',              count(*)               from INVENTORY_DATA
    union all select 'PRODUCTS_DATA_SNOWPIPE',      count(*)               from PRODUCTS_DATA_SNOWPIPE
    union all select 'CUSTOMERS_DATA_SNOWPIPE',     count(*)               from CUSTOMERS_DATA_SNOWPIPE
    union all select 'STORES_DATA_SNOWPIPE',        count(*)               from STORES_DATA_SNOWPIPE
    union all select 'SUPPLIERS_DATA_SNOWPIPE',     count(*)               from SUPPLIERS_DATA_SNOWPIPE
    union all select 'PROMOTIONS_DATA_SNOWPIPE',    count(*)               from PROMOTIONS_DATA_SNOWPIPE
)

select
    e.table_name,
    a.actual_rows,
    e.min_rows,
    e.max_rows,
    case
        when a.actual_rows < e.min_rows then 'UNDER_MIN'
        when a.actual_rows > e.max_rows then 'OVER_MAX'
        else 'OK'
    end as status
from expected_counts e
join actual_counts a using (table_name)
order by status, table_name;
