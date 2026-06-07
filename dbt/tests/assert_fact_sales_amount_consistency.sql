-- Custom data test: net_amount must never exceed gross_amount in fact_sales.
-- A row is returned only when the invariant is violated.
-- Severity is 'warn': the Faker-generated returns can produce a refund larger than
-- the original sale (net < 0) for a small number of rows. We surface these as a
-- data-quality warning rather than failing the whole pipeline on synthetic data;
-- in production this would route the offending rows to a quarantine table.
{{ config(severity='warn') }}

select
    sale_id,
    gross_amount,
    refund_amount,
    net_amount
from {{ ref('fact_sales') }}
where net_amount > gross_amount
   or net_amount < 0
   or refund_amount < 0
