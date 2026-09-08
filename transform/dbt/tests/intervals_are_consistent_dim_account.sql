-- Gate 4, for dim_account. Four failures, one test, because they are one property: the
-- versions of a natural key have to tile the timeline exactly once.
--
-- An overlap is what facts.py raises Multiplied for - every entry matches both versions,
-- which does not raise in the join, it multiplies. A gap is the quieter failure: an
-- entry dated inside it matches no version and drops out with nothing raised. Two rows
-- at the sentinel is an overlap that has not happened yet. And valid_from > valid_to
-- matches nothing at all, so the version silently stops existing.
--
-- See docs/adr/0024 (the open interval ends at a sentinel) and 0029.

with versions as (
    select
        account_code as natural_key,
        valid_from,
        valid_to,
        lead(valid_from) over (partition by account_code order by valid_from) as next_from
    from {{ ref('dim_account') }}
),

inverted as (
    select natural_key, valid_from, valid_to, 'inverted interval' as problem
    from versions
    where valid_from > valid_to
),

overlapping as (
    select natural_key, valid_from, valid_to, 'overlaps the next version' as problem
    from versions
    where next_from is not null and next_from <= valid_to
),

gapped as (
    select natural_key, valid_from, valid_to, 'leaves a gap before the next version' as problem
    from versions
    where next_from is not null and next_from > valid_to + interval '1 day'
),

currents as (
    select natural_key, min(valid_from) as valid_from, max(valid_to) as valid_to,
           'has ' || count(*) || ' current versions, expected 1' as problem
    from versions
    where valid_to = date '9999-12-31'
    group by natural_key
    having count(*) <> 1
)

select * from inverted
union all select * from overlapping
union all select * from gapped
union all select * from currents
