-- Gate 5. The current row count against the median of the previous builds.
--
-- The baseline excludes the current invocation. Included, the current count contributes
-- to the median it is being compared against, which drags the baseline toward whatever
-- happened and weakens the gate exactly when it should fire hardest - a single run that
-- halves every table.
--
-- Below drift_window previous builds the gate passes: a gate with one prior observation
-- would fire on the second build of a fresh clone, and a gate whose first real use is a
-- false alarm is one people learn to ignore.
--
-- The median rather than the mean, because one legitimately large backfill should not
-- raise the baseline enough to hide the shrink that follows it.
--
-- See docs/adr/0036.

{% set window = var('drift_window') %}
{% set tolerance = var('drift_tolerance') %}

with current_build as (
    select model, row_count
    from {{ ref('model_row_count') }}
    where invocation_id = '{{ invocation_id }}'
),

history as (
    select
        model,
        row_count,
        row_number() over (partition by model order by built_at desc) as recency
    from {{ ref('model_row_count') }}
    where invocation_id <> '{{ invocation_id }}'
),

recent as (
    select * from history where recency <= {{ window }}
),

baseline as (
    select
        model,
        count(*) as builds,
        -- Cast, because percentile_cont returns double precision and Postgres has no
        -- round(double, int). A ratio of row counts should be exact anyway: this gate
        -- is about counting, and a float is the wrong instrument for it.
        (percentile_cont(0.5) within group (order by row_count))::numeric as median_count
    from recent
    group by model
)

select
    current_build.model,
    current_build.row_count,
    baseline.median_count,
    baseline.builds,
    round(current_build.row_count / nullif(baseline.median_count, 0), 4) as ratio,
    'row count is ' || current_build.row_count || ' against a median of '
        || baseline.median_count || ' over ' || baseline.builds || ' builds' as problem
from current_build
join baseline on baseline.model = current_build.model
where baseline.builds >= {{ window }}
  and (
      baseline.median_count = 0
      -- The band is computed in SQL rather than in Jinja: Jinja would do it in
      -- binary floating point, and a tolerance of 0.10 that renders as
      -- 0.8999999999999999 is a gate whose edge is not where it says it is.
      or current_build.row_count / baseline.median_count
             not between (1 - {{ tolerance }}::numeric) and (1 + {{ tolerance }}::numeric)
  )
