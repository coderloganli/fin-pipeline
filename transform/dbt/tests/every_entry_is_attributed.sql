-- Gate 6. The fact holds exactly as many lines as the ledger it came from.
--
-- This is the gate that replaces two guards the attribution used to raise for itself.
-- The point-in-time join is `natural key AND date inside the interval`, three times
-- over, and it fails in two directions:
--
--   Too many. With the range alone - or with overlapping intervals in a dimension -
--   every entry matches every version valid on its date. That does not raise: it
--   multiplies, and the total still adds up to a number somebody might publish.
--
--   Too few. With a gap in a dimension's intervals, or a code the dimension never
--   carried, an entry matches no version and drops out of the join with nothing said.
--   An inner join is silent about what it discarded, which is why this is a gate and
--   not a comment.
--
-- Counted against the source table, so it is not a recomputation of the same figures:
-- the ledger has as many lines as it has, and attribution is not allowed to change that.
-- The amounts in their own currency are carried through for the same reason - a join
-- that duplicated a row would double them.

with source as (
    select 'gl_entry' as model, count(*) as lines,
           sum(amount_dr::numeric) as debit, sum(amount_cr::numeric) as credit
    from {{ source('landing', 'gl_entry') }}
    union all
    select 'gl_adjustment', count(*),
           sum(amount_dr::numeric), sum(amount_cr::numeric)
    from {{ source('landing', 'gl_adjustment') }}
),

fact as (
    select 'gl_entry' as model, count(*) as lines,
           sum(amount_dr) as debit, sum(amount_cr) as credit
    from {{ ref('fct_gl_entry') }}
    union all
    select 'gl_adjustment', count(*), sum(amount_dr), sum(amount_cr)
    from {{ ref('fct_gl_adjustment') }}
)

select
    source.model,
    source.lines as source_lines,
    fact.lines as fact_lines,
    source.debit as source_debit,
    fact.debit as fact_debit,
    case when fact.lines > source.lines then 'the join multiplied'
         when fact.lines < source.lines then 'the join dropped lines'
         else 'the amounts do not agree' end as problem
from source
join fact using (model)
where source.lines <> fact.lines
   or coalesce(source.debit, 0) <> coalesce(fact.debit, 0)
   or coalesce(source.credit, 0) <> coalesce(fact.credit, 0)
