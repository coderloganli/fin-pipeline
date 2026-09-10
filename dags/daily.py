"""The nightly run: validate, load, recompute, land, build, clear.

A declaration and nothing else. The order, the stop-on-failure rule, what each step
records and the fact that `clear-affected` runs last all live in `pipeline/`, which the
test suite exercises directly - this file only says when it happens and how the tasks
are wired. See docs/adr/0046.

Every `task_id` is a string literal naming its step, and the close task carries the
literal `trigger_rule=TriggerRule.ALL_DONE`. `tests/test_dags.py` reads this file with
`ast` and asserts those literals, which is how a step added to `pipeline.steps` and not
to this file is caught rather than quietly never running.
"""

import pendulum
from airflow.sdk import DAG, TriggerRule, task

from pipeline import run as runner
from pipeline import steps as step_list

PIPELINE = "daily"

with DAG(
    dag_id="daily",
    description="The nightly run, from the source extract to the built mart.",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["fin-pipeline"],
) as dag:

    @task(task_id="open-run")
    def open_run(**context) -> str:
        """The run identifier, which every task below carries. It is the platform's
        own; Airflow's goes on the record beside it. See docs/adr/0045."""
        return runner.open_run(
            runner.Context(),
            command=PIPELINE,
            steps=step_list.DAILY,
            orchestrator="airflow",
            orchestrator_run_id=context["run_id"],
        )

    @task(task_id="validate")
    def validate(run_id: str) -> str:
        runner.run_step(runner.Context(), run_id, step_list.VALIDATE)
        return run_id

    @task(task_id="load")
    def load(run_id: str) -> str:
        runner.run_step(runner.Context(), run_id, step_list.LOAD)
        return run_id

    @task(task_id="recompute")
    def recompute(run_id: str) -> str:
        runner.run_step(runner.Context(), run_id, step_list.RECOMPUTE)
        return run_id

    @task(task_id="mart-load")
    def mart_load(run_id: str) -> str:
        runner.run_step(runner.Context(), run_id, step_list.MART_LOAD)
        return run_id

    @task(task_id="dbt-build")
    def dbt_build(run_id: str) -> str:
        runner.run_step(runner.Context(), run_id, step_list.DBT_BUILD)
        return run_id

    @task(task_id="clear-affected")
    def clear_affected(run_id: str) -> str:
        runner.run_step(runner.Context(), run_id, step_list.CLEAR_AFFECTED)
        return run_id

    @task(task_id="close-run", trigger_rule=TriggerRule.ALL_DONE)
    def close_run(run_id: str, **context) -> None:
        """Runs however the run ended. What "however it ended" means is the runner's
        rule, not this file's - see docs/adr/0046."""
        runner.finalise(runner.Context(), run_id)

    opened = open_run()
    closed = close_run(opened)
    step = opened
    for stage in (validate, load, recompute, mart_load, dbt_build, clear_affected):
        step = stage(step)
    step >> closed
