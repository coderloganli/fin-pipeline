-- Gate 6. The base-currency amount against the original amount times the rate.
--
-- Worth being honest about what this is: the mart copies figures Spark computed with
-- the same rate, so it is not an independent recomputation of the conversion. It is a
-- gate on the load path and on the numeric types, and what it stops is a COPY that
-- mangled a decimal or a column that arrived as a float.
--
-- The tolerance is half a cent because docs/adr/0031 rounds at the line to two places.
-- A gate that fired on that rounding would fire on correct data, every build.

-- Both facts. An adjustment crosses the same load path and is converted by the same
-- code, so the gate that stops a mangled decimal has to cover it too. See docs/adr/0042.

with lines as (
    select
        'fct_gl_entry' as source_model,
        entry_id,
        version,
        amount_dr,
        amount_cr,
        rate_to_base,
        amount_dr_base,
        amount_cr_base,
        abs(amount_dr_base - amount_dr * rate_to_base) as debit_gap,
        abs(amount_cr_base - amount_cr * rate_to_base) as credit_gap
    from {{ ref('fct_gl_entry') }}

    union all

    select
        'fct_gl_adjustment' as source_model,
        entry_id,
        version,
        amount_dr,
        amount_cr,
        rate_to_base,
        amount_dr_base,
        amount_cr_base,
        abs(amount_dr_base - amount_dr * rate_to_base) as debit_gap,
        abs(amount_cr_base - amount_cr * rate_to_base) as credit_gap
    from {{ ref('fct_gl_adjustment') }}
)

select *
from lines
where debit_gap > 0.005 or credit_gap > 0.005
