"""The steps a run is made of, declared as data.

Each step is a name and something to call. The name is what the run record holds, what
a DAG file declares a task for, and what somebody reads at eight in the morning; the
callable is a thin adapter over a module that already does the work, because the work
belongs to `ingest/` and `transform/` and this package only decides the order.

`DAILY` is the whole of a nightly run. `BACKFILL` is the last four of it over an
explicit period range, for the case `docs/adr/0016` describes: an update that arrived
after the watermark window closed, or a range somebody has reason to rebuild. It passes
`force`, because `transform.backfill.run` otherwise returns without doing anything when
the affected-period set is empty - and the reason to type a range by hand is precisely
that the set does not name it.

**`dbt-build` builds into a schema of the run's own and renames it into place.** That is
why it needs `context.run_id`: the schema is named after the run so the two can be read
against each other. See docs/adr/0048.

**`judge` runs after `dbt-build` and before `clear-affected`.** After, because it trains
on the published mart (docs/adr/0051) and because a model that fails must not be able to
stop the mart being published - by the time it runs, the figures are already out.
Before, because on a daily run the affected set is how it knows which periods to judge,
and after the clear there is nothing left to read. See docs/adr/0050.

**`clear-affected` is last.** The set is owed until everything downstream of it has been
rebuilt, so a run that dies at `dbt-build` leaves it owed and the next run redoes the
work. That is the same ordering argument as the watermark moving last in
`docs/adr/0016`: what is cleared first is what goes missing when the run dies.

See docs/adr/0046.
"""

from dataclasses import dataclass
from typing import Callable

__all__ = ["Step", "MissingRunId", "VALIDATE", "LOAD", "RECOMPUTE", "MART_LOAD",
           "DBT_BUILD", "JUDGE", "CLEAR_AFFECTED", "DAILY", "BACKFILL", "by_name"]


@dataclass(frozen=True)
class Step:
    """One step: what it is called, and what running it does.

    `run` takes the run's context and returns the detail the record should carry. It
    raises to fail: the runner is what turns that into a recorded failure and a stopped
    run, so a step never has to know it is inside one.
    """

    name: str
    run: Callable[["object"], dict]


# --- what each step does ---------------------------------------------------

def _validate(context) -> dict:
    from ingest import validate

    report = validate.validate_source(context.source_dir)
    if report.incompatible:
        # docs/adr/0012: the library reports and the caller decides. Here the caller is
        # a nightly run, and docs/product.md's "breaking is better than drifting" is
        # what decides - a pipeline that keeps going and reports a wrong number is
        # worse than one that fails.
        raise ValidationFailed(report.describe())
    return {
        "tables": sorted(table.table for table in report.tables),
        "rows_read": sum(table.rows_read for table in report.tables),
        "warnings": [finding.message for finding in report.warnings],
    }


def _load(context) -> dict:
    from ingest import load

    report = load.load_source(
        context.source_dir, context.raw_dir,
        run_id=context.run_id, run_log=context.run_log,
    )
    return load.step_detail(report)


def _recompute(context) -> dict:
    from pipeline.run import spark_session
    from transform import backfill

    written = backfill.run(
        spark_session(context), context.raw_dir, context.staging_dir,
        periods=context.periods, force=context.force,
    )
    return {"periods": sorted(written)}


def _mart_load(context) -> dict:
    from transform import load as mart_load

    landed = mart_load.load_all(
        staging_dir=context.staging_dir, raw_dir=context.raw_dir,
        schema=context.landing_schema,
    )
    return {"rows": landed}


def _dbt_build(context) -> dict:
    from pipeline import dbt

    if not context.run_id:
        # The build schema is named after the run, and inventing one would build into a
        # schema no record mentions - which is the opposite of what docs/adr/0048 is
        # for. `run_step` sets this for every step under both entry points, so a context
        # without it is a caller that reached here some other way.
        raise MissingRunId(
            "dbt-build needs the run's run_id to name the schema it builds into; the "
            "context carries none. Steps are run through pipeline.run.run_step, which "
            "sets it."
        )
    return dbt.build_and_promote(
        landing=context.landing_schema, mart=context.mart_schema,
        run_id=context.run_id,
    )


def _judge(context) -> dict:
    """Flag the balances that fell outside their interval, for the periods this run
    touched.

    After `dbt-build`, because the model trains on the published mart rather than on the
    landing layer - the mart is the last set of figures that passed all six gates, and
    training upstream of them is a way for rejected figures to reach a reader anyway.
    See docs/adr/0051.

    Which periods is worked out here rather than handed over by `recompute`.
    `docs/adr/0046` keeps step-to-step handoff out of the DAG, so this step reads what it
    needs for itself.

    **`force` is what separates the two cases, not whether a range was given.** A daily
    run carries `--periods` too - it is the reporting range, and `recompute` cannot run
    without one - so branching on the range would have every nightly run re-judge three
    years. `force` is set by `python -m pipeline backfill` and by the backfill DAG's
    `context_for`, and it means what it means in `transform.backfill.run`: rebuild the
    range I was given whether or not anything is owed.

    So a backfill judges its range, and a daily run judges the affected set widened by
    the same closure `recompute` used - called rather than restated, because a window
    rule written down twice is one that stops being backfilled when it widens
    (docs/adr/0040). The closure is capped by the end of the reporting range, because a
    period outside it has no row in the mart to judge.
    """
    from ingest import affected
    from ml import judge
    from transform import db
    from transform.spark import balances

    if not context.run_id:
        raise MissingRunId(
            "judge needs the run's run_id: every flag carries the run that produced it, "
            "which is how a reader gets back to the mart build it was computed against. "
            "Steps are run through pipeline.run.run_step, which sets it."
        )

    first, last = balances.parse_periods(context.periods) if context.periods else (None, None)

    if context.force:
        periods = balances.period_range(first, last)
    else:
        owed = affected.read(context.raw_dir)
        if not owed.periods:
            return {"periods": [], "flagged": 0, "intervals_crossed": 0}
        closure = balances.dirty_closure(
            set(owed.periods), last_period=last or max(owed.periods)
        )
        periods = sorted(p for p in closure if first is None or p >= first)

    with db.connection_from() as connection:
        detail = judge.run(
            connection=connection,
            mart_schema=context.mart_schema,
            anomaly_schema=context.anomaly_schema,
            periods=periods,
            run_id=context.run_id,
        )
    # `not_judged` reaches the run record too. A period with no history in front of it
    # is not a period that was judged and found clean, and an operator reading
    # `flagged: 0` at eight in the morning has to be able to tell which happened.
    return {"periods": detail.periods, "flagged": detail.flagged,
            "intervals_crossed": detail.intervals_crossed,
            "not_judged": detail.not_judged}


def _clear_affected(context) -> dict:
    from ingest import affected

    # Read before clearing. `clear` returns nothing, so a step that only called it could
    # say that something was cleared but not what - and what was owed is the useful half
    # when somebody is working out why a period looks stale.
    owed = affected.read(context.raw_dir)
    affected.clear(context.raw_dir)
    return {"periods": list(owed.periods),
            "dimension_versions": list(owed.dimension_versions)}


class ValidationFailed(RuntimeError):
    """A source extract no longer matches its contract. See docs/adr/0009 and 0012."""


class MissingRunId(RuntimeError):
    """A step that needs the run's identifier was handed a context without one."""


# --- the steps, and the pipelines they make --------------------------------

VALIDATE = Step(name="validate", run=_validate)
LOAD = Step(name="load", run=_load)
RECOMPUTE = Step(name="recompute", run=_recompute)
MART_LOAD = Step(name="mart-load", run=_mart_load)
DBT_BUILD = Step(name="dbt-build", run=_dbt_build)
JUDGE = Step(name="judge", run=_judge)
CLEAR_AFFECTED = Step(name="clear-affected", run=_clear_affected)

DAILY = [VALIDATE, LOAD, RECOMPUTE, MART_LOAD, DBT_BUILD, JUDGE, CLEAR_AFFECTED]
BACKFILL = [RECOMPUTE, MART_LOAD, DBT_BUILD, JUDGE, CLEAR_AFFECTED]

PIPELINES = {"daily": DAILY, "backfill": BACKFILL}


def by_name(pipeline: str, step: str) -> Step:
    """One step of one pipeline, by the names a DAG task carries.

    A DAG task holds strings, not objects - it is a declaration in another process. This
    is how it gets back to the step those strings name, and it refuses an unknown one
    rather than skipping it: a task naming a step that does not exist has to fail where
    it is declared, not quietly do nothing.
    """
    steps = PIPELINES.get(pipeline)
    if steps is None:
        raise KeyError(f"no pipeline {pipeline!r}; there is {sorted(PIPELINES)}")
    for one in steps:
        if one.name == step:
            return one
    raise KeyError(
        f"{pipeline} has no step {step!r}; it has {[one.name for one in steps]}"
    )
