-- Custom data test: no sale should be dated in the future.
-- Returns offending rows; non-empty result = test failure.

select
    sale_id,
    sale_date,
    customer_id
from {{ ref('fact_sales') }}
where sale_date > current_date()
