-- assert_data_freshness.sql
-- Flags any raw table whose most recent ingestion is older than the SLA.
-- SLA: transactional = 6h, reference = 7d.
-- Used by the Airflow data_quality task and ad-hoc operations checks.

USE ROLE INGEST;
USE DATABASE INGEST;
USE SCHEMA INGEST;

with freshness as (
    select 'SALES_DATA'              as table_name, max(CREATED_AT)   as last_loaded_at, 6   as sla_hours from SALES_DATA
    union all select 'RETURNS_DATA',                max(CREATED_AT),                       6   from RETURNS_DATA
    union all select 'REVIEWS_DATA',                max(CREATED_AT),                       24  from REVIEWS_DATA
    union all select 'INVENTORY_DATA',              max(CREATED_AT),                       12  from INVENTORY_DATA
    union all select 'PRODUCTS_DATA_SNOWPIPE',      max(LAST_UPDATED)::timestamp_ntz,      168 from PRODUCTS_DATA_SNOWPIPE
    union all select 'CUSTOMERS_DATA_SNOWPIPE',     max(REGISTRATION_DATE)::timestamp_ntz, 168 from CUSTOMERS_DATA_SNOWPIPE
)

select
    table_name,
    last_loaded_at,
    sla_hours,
    timestampdiff('hour', last_loaded_at, current_timestamp()) as hours_since_last_load,
    case
        when last_loaded_at is null then 'EMPTY'
        when timestampdiff('hour', last_loaded_at, current_timestamp()) > sla_hours then 'STALE'
        else 'FRESH'
    end as freshness_status
from freshness
order by hours_since_last_load desc nulls first;
