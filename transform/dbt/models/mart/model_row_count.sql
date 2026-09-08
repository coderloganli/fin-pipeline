{{ config(materialized='incremental', unique_key=none, full_refresh=false) }}

-- One row per counted model per build. The baseline the drift gate compares against.
--
-- It depends on every model it counts and it does not count itself: a model counting
-- its own output in the same invocation would depend on itself, and the number of rows
-- in a table of row counts is not a figure anyone reports.
--
-- invocation_id is dbt's own, so a build identifies itself without a second identifier
-- being invented - and it lives here rather than on a mart row, because a column that
-- changes every build cannot coexist with the reproducibility criterion.
--
-- full_refresh=false because this table is a record rather than a derivation. Every
-- other model here can be rebuilt from its inputs; this one cannot, and `--full-refresh`
-- would drop the history the drift gate compares against - leaving the gate with no
-- baseline and therefore silent, on exactly the run where an operator reached for
-- --full-refresh because something looked wrong.
--
-- See docs/adr/0036 and 0038.

{% set counted = [
    'fct_gl_entry',
    'fct_gl_adjustment',
    'agg_monthly_balance',
    'dim_account',
    'dim_cost_center',
    'dim_fx_rate',
    'dim_vendor',
] %}

{% for model in counted %}
select
    '{{ invocation_id }}'::text as invocation_id,
    now() as built_at,
    '{{ model }}'::text as model,
    count(*)::bigint as row_count
from {{ ref(model) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
