-- The monthly balance, widened.
--
-- The names are taken as of the period's last day, and that is not a new rule. The
-- landing aggregate has no surrogate keys - transform/spark/balances.py groups by the
-- natural codes - so joining an SCD2 dimension on the code alone would match every
-- version valid in any period, which does not raise: it multiplies, and the result
-- still totals to a number somebody might publish.
--
-- The as-of date is the period's close because that is the date balances.py already
-- reports the account type as of. A name resolved at a different date would put two
-- answers in one row. See docs/adr/0032.
--
-- No vendor_name: this grain has no vendor, and a name here would be an invention.

with balances as (
    select
        *,
        (to_date(accounting_period, 'YYYY-MM') + interval '1 month' - interval '1 day')::date
            as period_end
    from {{ source('landing', 'agg_monthly_balance') }}
)

select
    balances.account_code,
    account.name as account_name,
    balances.cost_center_code,
    cost_center.name as cost_center_name,
    cost_center.dept_code,
    balances.accounting_period,
    balances.account_type,

    balances.debit_total,
    balances.credit_total,
    balances.balance,

    balances.balance_delta_mom,
    balances.balance_pct_mom,
    balances.balance_delta_yoy,
    balances.balance_pct_yoy,
    balances.balance_rolling_3m,
    balances.rolling_periods

from balances
left join {{ ref('dim_account') }} as account
    on account.account_code = balances.account_code
   and balances.period_end between account.valid_from and account.valid_to
left join {{ ref('dim_cost_center') }} as cost_center
    on cost_center.cc_code = balances.cost_center_code
   and balances.period_end between cost_center.valid_from and cost_center.valid_to
