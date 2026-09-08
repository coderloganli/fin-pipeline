-- The entry fact, widened so the application computes nothing.
--
-- The three surrogate keys are non-null in the landing table - transform/spark/facts.py
-- raises Unattributed otherwise - so the dimension joins are equality joins on the key,
-- not range joins repeated here. The vendor has no surrogate key, because an entry is
-- not attributed to a vendor version; it joins on vendor_code, and it joins left,
-- because a sale is earned from a customer rather than paid to a supplier.
--
-- `nullif(vendor_code, '')` is where an empty field becomes absence. Raw lands every
-- column as text and writes an empty field as an empty string - docs/adr/0015 - because
-- raw exists to answer whether the source really said that, and the staging fact carries
-- that text through. The mart is the layer the application reads, so it is where the
-- empty string becomes the null that ingest/contracts already says it means. Without
-- this, every sale carries a vendor code of '' that matches no supplier, and the
-- referential integrity gate is red on a correct ledger.
--
-- No column names the build that wrote this. See docs/adr/0038.

with entries as (
    select
        *,
        nullif(vendor_code, '') as supplier_code,
        nullif(parent_code, '') as parent
    from {{ source('landing', 'fct_gl_entry') }}
)

select
    entries.entry_id,
    entries.version,
    entries.accounting_date,
    entries.posted_at,

    entries.account_key,
    entries.cost_center_key,
    entries.fx_key,

    entries.account_code,
    account.name as account_name,
    entries.account_type,
    entries.parent as parent_code,

    entries.cost_center_code,
    cost_center.name as cost_center_name,
    entries.dept_code,

    entries.currency,
    entries.rate_to_base,
    entries.amount_dr,
    entries.amount_cr,
    entries.amount_dr_base,
    entries.amount_cr_base,

    entries.doc_id,
    entries.supplier_code as vendor_code,
    vendor.name as vendor_name,
    entries.description,

    entries.source_first_run_id,
    entries.source_last_run_id

from entries
left join {{ ref('dim_account') }} as account
    on account.surrogate_key = entries.account_key
left join {{ ref('dim_cost_center') }} as cost_center
    on cost_center.surrogate_key = entries.cost_center_key
left join {{ ref('dim_vendor') }} as vendor
    on vendor.vendor_code = entries.supplier_code
