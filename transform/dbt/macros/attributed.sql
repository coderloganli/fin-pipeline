{# One join shape, used three times per fact: the natural key, and the date inside the
   version's interval.

       <natural key> = <natural key>  AND  accounting_date BETWEEN valid_from AND valid_to

   Both halves, every time. With the range alone every entry matches every version valid
   on its date, which does not raise - it multiplies, and the result still adds up to a
   number somebody might publish. See docs/adr/0029. #}

{% macro entry_facts(source_table, extra_columns=[]) %}

with source as (
    select
        entry_id,
        version,
        accounting_date::date as accounting_date,
        accounting_period,
        posted_at::date as posted_at,
        account_code,
        cost_center_code,
        currency,
        amount_dr::numeric(18,2) as amount_dr,
        amount_cr::numeric(18,2) as amount_cr,
        doc_id,
        {% for column in extra_columns %}{{ column }},{% endfor %}
        -- Where an empty field becomes absence. Raw lands every column as text and
        -- writes an empty field as an empty string, because raw exists to answer whether
        -- the source really said that. The mart is the layer the application reads, so
        -- it is where the empty string becomes the null that ingest/contracts already
        -- says it means. Without this, every sale carries a vendor code of '' that
        -- matches no supplier and the referential integrity gate is red on a correct
        -- ledger. See docs/adr/0015.
        nullif(vendor_code, '') as vendor_code,
        description,
        _first_run_id as source_first_run_id,
        _last_run_id as source_last_run_id
    from {{ source('landing', source_table) }}
)

select
    source.entry_id,
    source.version,
    source.accounting_date,
    -- The period the entry belongs to, carried through from the raw partition. It is
    -- what agg_monthly_balance is keyed by, so a query that drills from a balance to its
    -- entries joins on it rather than re-deriving it from the date.
    source.accounting_period,
    source.posted_at,

    account.surrogate_key as account_key,
    cost_center.surrogate_key as cost_center_key,
    fx.surrogate_key as fx_key,

    source.account_code,
    account.name as account_name,
    account.account_type,
    nullif(account.parent_code, '') as parent_code,

    source.cost_center_code,
    cost_center.name as cost_center_name,
    cost_center.dept_code,

    source.currency,
    fx.rate_to_base,
    source.amount_dr,
    source.amount_cr,
    -- Rounded at the line, not at the total: the monthly aggregate sums figures a
    -- reader can tie back to a voucher. See docs/adr/0031.
    round(source.amount_dr * fx.rate_to_base, 2)::numeric(32,2) as amount_dr_base,
    round(source.amount_cr * fx.rate_to_base, 2)::numeric(32,2) as amount_cr_base,

    source.doc_id,
    {% for column in extra_columns %}source.{{ column }},{% endfor %}
    source.vendor_code,
    vendor.name as vendor_name,
    source.description,

    source.source_first_run_id,
    source.source_last_run_id

from source
join {{ ref('dim_account') }} as account
    on account.account_code = source.account_code
   and source.accounting_date between account.valid_from and account.valid_to
join {{ ref('dim_cost_center') }} as cost_center
    on cost_center.cc_code = source.cost_center_code
   and source.accounting_date between cost_center.valid_from and cost_center.valid_to
join {{ ref('dim_fx_rate') }} as fx
    on fx.currency = source.currency
   and source.accounting_date between fx.valid_from and fx.valid_to
-- Left, because a sale is earned from a customer rather than paid to a supplier.
left join {{ ref('dim_vendor') }} as vendor
    on vendor.vendor_code = source.vendor_code

{% endmacro %}
