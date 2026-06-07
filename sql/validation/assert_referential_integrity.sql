-- assert_referential_integrity.sql
-- Returns any rows that violate cross-table foreign keys in the raw layer.
-- Used as an ad-hoc audit query; the dbt schema.yml relationships tests cover
-- the canonical case.
-- Exit code is non-zero if any row is returned (use with `snowsql -q` + a wrapper).

USE ROLE INGEST;
USE DATABASE INGEST;
USE SCHEMA INGEST;

with orphan_returns as (
    select 'returns_without_sale' as check_name, count(*) as offending_rows
    from RETURNS_DATA r
    where not exists (select 1 from SALES_DATA s where s.SALE_ID = r.SALE_ID)
),

orphan_reviews as (
    select 'reviews_without_product' as check_name, count(*) as offending_rows
    from REVIEWS_DATA r
    where not exists (select 1 from PRODUCTS_DATA_SNOWPIPE p where p.PRODUCT_ID = r.PRODUCT_ID)
),

orphan_sales_customer as (
    select 'sales_without_customer' as check_name, count(*) as offending_rows
    from SALES_DATA s
    where not exists (select 1 from CUSTOMERS_DATA_SNOWPIPE c where c.CUSTOMER_ID = s.CUSTOMER_ID)
),

orphan_sales_product as (
    select 'sales_without_product' as check_name, count(*) as offending_rows
    from SALES_DATA s
    where not exists (select 1 from PRODUCTS_DATA_SNOWPIPE p where p.PRODUCT_ID = s.PRODUCT_ID)
)

select * from orphan_returns
union all select * from orphan_reviews
union all select * from orphan_sales_customer
union all select * from orphan_sales_product
order by offending_rows desc;
