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

**`clear-affected` is last.** The set is owed until everything downstream of it has been
rebuilt, so a run that dies at `dbt-build` leaves it owed and the next run redoes the
work. That is the same ordering argument as the watermark moving last in
`docs/adr/0016`: what is cleared first is what goes missing when the run dies.

See docs/adr/0046.
"""

from dataclasses import dataclass
from typing import Callable

__all__ = ["Step", "VALIDATE", "LOAD", "RECOMPUTE", "MART_LOAD", "DBT_BUILD",
           "CLEAR_AFFECTED", "DAILY", "BACKFILL", "by_name"]


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

    return dbt.build(landing=context.landing_schema, mart=context.mart_schema)


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


# --- the steps, and the pipelines they make --------------------------------

VALIDATE = Step(name="validate", run=_validate)
LOAD = Step(name="load", run=_load)
RECOMPUTE = Step(name="recompute", run=_recompute)
MART_LOAD = Step(name="mart-load", run=_mart_load)
DBT_BUILD = Step(name="dbt-build", run=_dbt_build)
CLEAR_AFFECTED = Step(name="clear-affected", run=_clear_affected)

DAILY = [VALIDATE, LOAD, RECOMPUTE, MART_LOAD, DBT_BUILD, CLEAR_AFFECTED]
BACKFILL = [RECOMPUTE, MART_LOAD, DBT_BUILD, CLEAR_AFFECTED]

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
