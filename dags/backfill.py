"""Rebuilding a range of accounting periods, on demand.

The last four steps of the daily run over an explicit range, triggered by hand rather
than scheduled. It rebuilds that range whether or not the affected-period set names it,
because the reason to type a range is precisely that the set does not: a bug that has
been fixed, or an update that arrived after the watermark window closed, which
docs/adr/0016 names as the case this recovers.

The range comes from the trigger's configuration - `{"periods": "2026-01:2026-06"}` -
because a backfill without one is a full rebuild nobody asked for.

A declaration and nothing else; see `dags/daily.py` and docs/adr/0046 for why, and for
the literal convention `tests/test_dags.py` reads this file against.
"""

import pendulum
from airflow.sdk import DAG, TriggerRule, task

from pipeline import run as runner
from pipeline import steps as step_list

PIPELINE = "backfill"


def context_for(params) -> runner.Context:
    """A backfill's context: the range it was given, and `force` because of it."""
    periods = (params or {}).get("periods")
    if not periods:
        raise ValueError(
            'a backfill needs a range: trigger it with {"periods": "YYYY-MM:YYYY-MM"}'
        )
    return runner.Context(periods=periods, force=True)


with DAG(
    dag_id="backfill",
    description="Rebuild an explicit range of accounting periods.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"periods": ""},
    default_args={"retries": 1},
    tags=["fin-pipeline"],
) as dag:

    @task(task_id="open-run")
    def open_run(**context) -> str:
        working = context_for(context["params"])
        return runner.open_run(
            working,
            command=PIPELINE,
            steps=step_list.BACKFILL,
            orchestrator="airflow",
            orchestrator_run_id=context["run_id"],
        )

    @task(task_id="recompute")
    def recompute(run_id: str, **context) -> str:
        runner.run_step(context_for(context["params"]), run_id, step_list.RECOMPUTE)
        return run_id

    @task(task_id="mart-load")
    def mart_load(run_id: str, **context) -> str:
        runner.run_step(context_for(context["params"]), run_id, step_list.MART_LOAD)
        return run_id

    @task(task_id="dbt-build")
    def dbt_build(run_id: str, **context) -> str:
        runner.run_step(context_for(context["params"]), run_id, step_list.DBT_BUILD)
        return run_id

    @task(task_id="judge")
    def judge(run_id: str, **context) -> str:
        runner.run_step(context_for(context["params"]), run_id, step_list.JUDGE)
        return run_id

    @task(task_id="clear-affected")
    def clear_affected(run_id: str, **context) -> str:
        runner.run_step(context_for(context["params"]), run_id,
                        step_list.CLEAR_AFFECTED)
        return run_id

    @task(task_id="close-run", trigger_rule=TriggerRule.ALL_DONE)
    def close_run(run_id: str, **context) -> None:
        """Runs however the run ended. What "however it ended" means is the runner's
        rule, not this file's - see docs/adr/0046."""
        runner.finalise(runner.Context(), run_id)

    opened = open_run()
    closed = close_run(opened)
    step = opened
    for stage in (recompute, mart_load, dbt_build, judge, clear_affected):
        step = stage(step)
    step >> closed
