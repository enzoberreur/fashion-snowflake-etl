-- Custom data test: net_amount must never exceed gross_amount in fact_sales.
-- A row is returned only when the invariant is violated, which fails the test.

select
    sale_id,
    gross_amount,
    refund_amount,
    net_amount
from {{ ref('fact_sales') }}
where net_amount > gross_amount
   or net_amount < 0
   or refund_amount < 0
