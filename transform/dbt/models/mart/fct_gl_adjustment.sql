-- The adjustment fact, widened so the application computes nothing.
--
-- The same shape as fct_gl_entry, and deliberately so: an adjustment is attributed by
-- the same three point-in-time joins, converted with the rate in force on its own
-- accounting date, and rounded at the line. What it adds is `adjusts_entry_id` - which
-- line it revises - and `adjustment_type`, which says whether it amends the period's
-- figure or restates it.
--
-- It is a table of its own rather than rows of fct_gl_entry because gate 3 groups by
-- doc_id and requires debits to equal credits. An adjustment is a single-sided delta
-- against a voucher that already balanced, so folding it in would turn that gate red on
-- a correct ledger. See docs/adr/0042.
--
-- `adjusts_entry_id` is carried and not enforced. An adjustment can legitimately point
-- at an entry outside the loaded window, and a gate that fires on a correct backfill is
-- one people learn to ignore. It is a trail to follow, not a constraint.

with adjustments as (
    select
        *,
        nullif(vendor_code, '') as supplier_code,
        nullif(parent_code, '') as parent
    from {{ source('landing', 'fct_gl_adjustment') }}
)

select
    adjustments.entry_id,
    adjustments.version,
    adjustments.accounting_date,
    adjustments.accounting_period,
    adjustments.posted_at,

    adjustments.account_key,
    adjustments.cost_center_key,
    adjustments.fx_key,

    adjustments.account_code,
    account.name as account_name,
    adjustments.account_type,
    adjustments.parent as parent_code,

    adjustments.cost_center_code,
    cost_center.name as cost_center_name,
    adjustments.dept_code,

    adjustments.currency,
    adjustments.rate_to_base,
    adjustments.amount_dr,
    adjustments.amount_cr,
    adjustments.amount_dr_base,
    adjustments.amount_cr_base,

    adjustments.doc_id,
    adjustments.adjusts_entry_id,
    adjustments.adjustment_type,
    adjustments.supplier_code as vendor_code,
    vendor.name as vendor_name,
    adjustments.description,

    adjustments.source_first_run_id,
    adjustments.source_last_run_id

from adjustments
left join {{ ref('dim_account') }} as account
    on account.surrogate_key = adjustments.account_key
left join {{ ref('dim_cost_center') }} as cost_center
    on cost_center.surrogate_key = adjustments.cost_center_key
left join {{ ref('dim_vendor') }} as vendor
    on vendor.vendor_code = adjustments.supplier_code
