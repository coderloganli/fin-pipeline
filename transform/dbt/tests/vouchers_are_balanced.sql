-- Gate 3. Two rows sharing a doc_id make a balanced voucher.
--
-- The contract for gl_entry says explicitly that this rule belongs here rather than
-- there, because it is a rule about a group of rows and a contract checks one row at a
-- time.
--
-- Over base-currency amounts, not original: a voucher whose two sides are in different
-- currencies balances in base and does not in original, so testing the original amounts
-- would fail a correct ledger.

select
    doc_id,
    sum(amount_dr_base) as debit_total,
    sum(amount_cr_base) as credit_total,
    sum(amount_dr_base) - sum(amount_cr_base) as difference
from {{ ref('fct_gl_entry') }}
group by doc_id
having sum(amount_dr_base) <> sum(amount_cr_base)
