-- The monthly balance, over a dense grid, signed by the account's normal side.
--
-- Every account's ordinary activity reads as a positive number that grows, so neither
-- the report nor the anomaly model has to know which way an account points - and the
-- sign comes from the account type that was in force in the period, so a
-- reclassification does not flip the sign of a period that already closed. See
-- docs/adr/0032.
--
-- Every active combination carries a row for every period in range, because a
-- period-over-period comparison over a sparse table silently becomes a comparison with
-- the last period that had activity rather than with last month. See docs/adr/0033.
--
-- The names are taken as of the period's last day. A name resolved at a different date
-- would put two answers in one row. See docs/adr/0032.

{% set debit_normal = "('asset', 'expense')" %}

with entries as (
    select account_code, cost_center_code, accounting_period, accounting_date,
           account_type, amount_dr_base, amount_cr_base, null::text as adjustment_type
    from {{ ref('fct_gl_entry') }}
),

adjustments as (
    select account_code, cost_center_code, accounting_period, accounting_date,
           account_type, amount_dr_base, amount_cr_base, adjustment_type
    from {{ ref('fct_gl_adjustment') }}
),

-- One row per combination that has activity anywhere, times every period in range.
-- `anywhere` is the whole fact table, and deliberately: density is a statement about
-- every period, so a membership set derived from a window would produce a grid with
-- holes in it. Corrections count towards membership - they are postings on the
-- as-reported basis - and so do restatements, because a combination that exists only on
-- the restated basis still needs a row.
identity as (
    select distinct account_code, cost_center_code from entries
    union
    select distinct account_code, cost_center_code from adjustments
),

span as (
    select min(accounting_period) as first, max(accounting_period) as last
    from (select accounting_period from entries
          union all select accounting_period from adjustments) as all_periods
),

grid_periods as (
    select to_char(month_start, 'YYYY-MM') as accounting_period
    from span,
         generate_series(to_date(span.first || '-01', 'YYYY-MM-DD'),
                         to_date(span.last || '-01', 'YYYY-MM-DD'),
                         interval '1 month') as month_start
),

grid as (
    select identity.account_code, identity.cost_center_code,
           grid_periods.accounting_period
    from identity cross join grid_periods
),

-- The as-reported basis: entries and `correction` adjustments. A correction amends the
-- period's figure, so after it lands there is one number and it is the amended one.
-- See docs/adr/0043.
reported as (
    select * from entries
    union all
    select * from adjustments where adjustment_type = 'correction'
),

posted as (
    select
        account_code, cost_center_code, accounting_period,
        sum(amount_dr_base)::numeric(38,2) as debit_total,
        sum(amount_cr_base)::numeric(38,2) as credit_total,
        -- One type per period, and it is the one in force at the period's close. An
        -- account reclassified mid-month has entries carrying both types; keeping both
        -- would match the same totals twice, duplicating the row and signing the two
        -- copies against each other. A month is reported as of its end.
        (array_agg(account_type order by accounting_date desc, account_type desc))[1]
            as closing_type
    from reported
    group by account_code, cost_center_code, accounting_period
),

-- The restatement side, kept apart. It is the bridge between the two bases and a figure
-- in its own right - the number an analyst asking "what changed" wants.
restated as (
    select
        account_code, cost_center_code, accounting_period,
        sum(amount_dr_base)::numeric(38,2) as restatement_debit,
        sum(amount_cr_base)::numeric(38,2) as restatement_credit
    from adjustments
    where adjustment_type = 'restatement'
    group by account_code, cost_center_code, accounting_period
),

-- A period with no entries has no account type of its own, so it borrows the one from
-- the nearest period that had entries. Without it the sign of a zero row would be
-- undefined - and a zero is signed the same either way, but the column should still say
-- what the account was.
dense as (
    select
        grid.account_code,
        grid.cost_center_code,
        grid.accounting_period,
        -- Postgres has no IGNORE NULLS, and a plain last_value over a grid full of
        -- nulls returns the null. So the nearest period that did post is picked by
        -- taking the type of the greatest period at or before this one, and failing
        -- that the least period after it: the same "borrow from the nearest" rule,
        -- expressed as an aggregate rather than as a navigation function.
        coalesce(
            (array_agg(posted.closing_type) filter (where posted.closing_type is not null) over (
                partition by grid.account_code, grid.cost_center_code
                order by grid.accounting_period
                rows between unbounded preceding and current row
            ))[array_upper(array_agg(posted.closing_type) filter (where posted.closing_type is not null) over (
                partition by grid.account_code, grid.cost_center_code
                order by grid.accounting_period
                rows between unbounded preceding and current row
            ), 1)],
            (array_agg(posted.closing_type) filter (where posted.closing_type is not null) over (
                partition by grid.account_code, grid.cost_center_code
                order by grid.accounting_period
                rows between current row and unbounded following
            ))[1]
        ) as account_type,
        coalesce(posted.debit_total, 0)::numeric(38,2) as debit_total,
        coalesce(posted.credit_total, 0)::numeric(38,2) as credit_total,
        coalesce(restated.restatement_debit, 0)::numeric(38,2) as restatement_debit,
        coalesce(restated.restatement_credit, 0)::numeric(38,2) as restatement_credit
    from grid
    left join posted using (account_code, cost_center_code, accounting_period)
    left join restated using (account_code, cost_center_code, accounting_period)
),

-- Two bases and the column that bridges them. `balance_as_reported` is entries and
-- corrections; `restatement_delta` is the restatements alone, signed the same way, and
-- it is the figure an analyst asking "what changed" actually wants. `balance_as_restated`
-- is their sum, stored rather than left to the reader. See docs/adr/0043.
based as (
    select
        *,
        (case when account_type in {{ debit_normal }}
              then debit_total - credit_total
              else credit_total - debit_total end)::numeric(38,2) as balance_as_reported,
        (case when account_type in {{ debit_normal }}
              then restatement_debit - restatement_credit
              else restatement_credit - restatement_debit end)::numeric(38,2)
            as restatement_delta
    from dense
),

bridged as (
    select *, (balance_as_reported + restatement_delta)::numeric(38,2) as balance_as_restated
    from based
),

-- A delta that is always defined, and a percentage that is not. The percentage is null
-- when the base is zero, which is exactly the case an anomaly investigation cares about
-- most - an account that had nothing and now has something. The denominator is an
-- absolute value: without it a balance moving from -100 to -50 divides by a negative and
-- reports as a fall.
--
-- Over the restated basis, because that is the answer a report shows.
compared as (
    select
        *,
        lag(balance_as_restated, {{ var('mom_lag', 1) }}) over w as _before_mom,
        lag(balance_as_restated, {{ var('yoy_lag', 12) }}) over w as _before_yoy,
        round(avg(balance_as_restated) over (
            partition by account_code, cost_center_code order by accounting_period
            rows between {{ var('rolling_span', 3) - 1 }} preceding and current row
        ), 2)::numeric(38,2) as balance_rolling_3m,
        count(balance_as_restated) over (
            partition by account_code, cost_center_code order by accounting_period
            rows between {{ var('rolling_span', 3) - 1 }} preceding and current row
        )::int as rolling_periods
    from bridged
    window w as (partition by account_code, cost_center_code order by accounting_period)
)

select
    compared.account_code,
    account.name as account_name,
    compared.cost_center_code,
    cost_center.name as cost_center_name,
    cost_center.dept_code,
    compared.accounting_period,
    compared.account_type,

    compared.debit_total,
    compared.credit_total,

    compared.balance_as_reported,
    compared.restatement_delta,
    compared.balance_as_restated,

    (compared.balance_as_restated - compared._before_mom)::numeric(38,2) as balance_delta_mom,
    (case when compared._before_mom = 0 then null
          else (compared.balance_as_restated - compared._before_mom) / abs(compared._before_mom)
     end)::numeric(18,6) as balance_pct_mom,
    (compared.balance_as_restated - compared._before_yoy)::numeric(38,2) as balance_delta_yoy,
    (case when compared._before_yoy = 0 then null
          else (compared.balance_as_restated - compared._before_yoy) / abs(compared._before_yoy)
     end)::numeric(18,6) as balance_pct_yoy,
    compared.balance_rolling_3m,
    compared.rolling_periods

from compared
-- The as-of date is the period's close, because that is the date the balance already
-- reports the account type as of. The landing aggregate has no surrogate keys, so joining
-- an SCD2 dimension on the code alone would match every version valid in any period,
-- which does not raise: it multiplies, and the result still totals to a number somebody
-- might publish.
cross join lateral (
    select (to_date(compared.accounting_period || '-01', 'YYYY-MM-DD')
            + interval '1 month' - interval '1 day')::date as period_end
) as p
left join {{ ref('dim_account') }} as account
    on account.account_code = compared.account_code
   and p.period_end between account.valid_from and account.valid_to
left join {{ ref('dim_cost_center') }} as cost_center
    on cost_center.cc_code = compared.cost_center_code
   and p.period_end between cost_center.valid_from and cost_center.valid_to
