"""The runner: what a run is, and what it writes down about itself.

A run is a sequence of named steps. `pipeline/steps.py` declares them and
`pipeline/run.py` executes them, opening the run record before the first one and
closing it after the last however it ended. The three primitives - `open_run`,
`run_step`, `close_run` - stay separate rather than folded into `run_pipeline` so that a
caller can drive the steps itself and the record still spans all of them. Nothing about
a run lives in the scheduler, which is why the scheduler can be a cron entry. See
docs/adr/0046.

`clear-affected` is the last step, after `dbt-build`. The affected-period set is owed
until everything downstream of it has been rebuilt, so a run that dies at dbt leaves it
owed and the next run redoes the work - the same ordering argument as the watermark
moving last in docs/adr/0016.

Cases 13-20 of orchestrate-the-daily-run.
"""

import pytest

from ingest import affected, runs
from pipeline import run as runner
from pipeline import steps as step_list


@pytest.fixture
def raw_dir(tmp_path):
    directory = tmp_path / "raw"
    directory.mkdir(parents=True)
    return directory


@pytest.fixture
def context(tmp_path, raw_dir):
    return runner.Context(
        source_dir=tmp_path / "source",
        raw_dir=raw_dir,
    )


def recorder(name, done, *, detail=None, fails=False):
    """A step that writes its name down when it runs, so order is observable."""
    def go(context):
        done.append(name)
        if fails:
            raise RuntimeError(f"{name} went wrong")
        return detail or {}
    return step_list.Step(name=name, run=go)


def steps_of(record):
    return [(step.step, step.status) for step in record.steps]


# --- the runner ------------------------------------------------------------

def test_the_steps_run_in_the_order_they_are_declared(raw_dir, context):
    """Case 13."""
    done = []
    plan = [recorder(name, done) for name in ("first", "second", "third")]

    runner.run_pipeline(context, command="daily", steps=plan)

    assert done == ["first", "second", "third"]


def test_a_failing_step_stops_the_run_and_is_named(raw_dir, context):
    """Case 14. The steps before it keep what they recorded: a record that only said
    the run failed would lose what it managed to do first."""
    done = []
    plan = [
        recorder("first", done),
        recorder("second", done, fails=True),
        recorder("third", done),
    ]

    with pytest.raises(RuntimeError, match="second went wrong"):
        runner.run_pipeline(context, command="daily", steps=plan)

    assert done == ["first", "second"]
    record = runs.RunLog(raw_dir).read()[0]
    assert record.status == "failed"
    assert record.failed_step == "second"
    assert "second went wrong" in record.error
    assert steps_of(record) == [("first", "succeeded"), ("second", "failed")]


def test_open_run_writes_one_started_event_and_returns_the_identifier(raw_dir, context):
    """Case 15. The identifier keeps the shape docs/adr/0019 fixed: it is what
    docs/adr/0018 stamps on rows, and a run started by hand has no orchestrator to
    borrow one from. See docs/adr/0045."""
    import re

    run_id = runner.open_run(context, command="daily", steps=step_list.DAILY)

    assert re.match(r"^\d{8}T\d{6}Z-[0-9a-f]{6}$", run_id)
    events = [event["event"] for event in read_events(raw_dir)]
    assert events == ["started"]


def test_the_orchestrator_is_recorded_beside_the_run(raw_dir, context, capsys):
    """The person with the problem is looking at whatever started the run - a cron log,
    a CI job - and one field takes them to the record while the same field takes them
    back."""
    run_id = runner.open_run(
        context, command="daily", steps=step_list.DAILY,
        orchestrator="cron",
        orchestrator_run_id="2026-09-08T03:00:00+00:00",
    )
    runner.close_run(context, run_id, status="succeeded")

    assert runs.main(["--raw", str(raw_dir), "--run", run_id]) == 0
    printed = capsys.readouterr().out
    assert "cron" in printed
    assert "2026-09-08T03:00:00+00:00" in printed


def test_close_run_writes_one_finished_event_and_refuses_a_second(raw_dir, context):
    """Case 17. A run writes one outcome. A second would silently replace the first,
    and which of the two is true is not something this can decide."""
    run_id = runner.open_run(context, command="daily", steps=step_list.DAILY)
    runner.close_run(context, run_id, status="succeeded")

    with pytest.raises(runs.RunLogError):
        runner.close_run(context, run_id, status="succeeded")


def test_run_step_records_the_detail_the_step_returns(raw_dir, context):
    """Case 18. `detail` is deliberately unstructured: each step has something
    different worth recording. See docs/adr/0044."""
    done = []
    run_id = runner.open_run(context, command="daily", steps=["counted"])

    returned = runner.run_step(
        context, run_id, recorder("counted", done, detail={"periods": ["2026-01"]}),
    )

    assert returned == {"periods": ["2026-01"]}
    record = runs.RunLog(raw_dir).read()[0]
    assert record.steps[0].detail == {"periods": ["2026-01"]}


# --- the affected-period set is cleared last -------------------------------

def test_a_failed_dbt_build_leaves_the_periods_owed(raw_dir, context):
    """Case 19. This is the ordering the design turns on. Clearing the set before the
    mart has been rebuilt from it would lose the work with nothing raised: the next run
    would see nothing owed and the periods would stay stale until a full rerun."""
    affected.record(raw_dir, periods=["2026-01", "2026-02"])
    done = []
    plan = [
        recorder("dbt-build", done, fails=True),
        step_list.CLEAR_AFFECTED,
    ]

    with pytest.raises(RuntimeError):
        runner.run_pipeline(context, command="daily", steps=plan)

    assert affected.read(raw_dir).periods == ["2026-01", "2026-02"]


def test_a_successful_run_clears_the_set(raw_dir, context):
    """Case 20. `transform.backfill` does not clear it - it says so itself, and names
    the orchestrator as the step that should. This is that step. See docs/adr/0039."""
    affected.record(raw_dir, periods=["2026-01", "2026-02"])
    done = []
    plan = [recorder("dbt-build", done), step_list.CLEAR_AFFECTED]

    runner.run_pipeline(context, command="daily", steps=plan)

    assert affected.read(raw_dir).is_empty()


def read_events(raw_dir):
    import json
    path = raw_dir / "_state" / "runs.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


# --- closing a run ---------------------------------------------------------

def test_a_step_that_succeeded_on_a_retry_is_recorded_as_a_second_attempt(
        raw_dir, context):
    """Re-running one step under the same run id is a second attempt, not a second run.
    The record keeps both, so a reader can tell a step that worked on the retry from one
    that never worked - and `interrupted` keeps meaning what it means, because nothing
    wrote an outcome."""
    done = []
    flaky = recorder("first", done, fails=True)
    run_id = runner.open_run(context, command="daily",
                             steps=[flaky, recorder("second", done)])
    with pytest.raises(RuntimeError):
        runner.run_step(context, run_id, flaky)
    runner.run_step(context, run_id, recorder("first", done))
    runner.run_step(context, run_id, recorder("second", done))

    record = runs.RunLog(raw_dir).read()[0]
    assert [(s.step, s.attempt, s.status) for s in record.steps] == [
        ("first", 1, "failed"), ("first", 2, "succeeded"), ("second", 1, "succeeded"),
    ]


def test_closing_a_run_that_never_started_is_refused(raw_dir, context):
    """Appending a `finished` for a run with no `started` would make the whole log
    unreadable from that line on. Refusing keeps the failure where the mistake is."""
    with pytest.raises(runs.RunLogError, match="no run"):
        runner.close_run(context, runs.new_run_id(), status="succeeded")

    assert not (raw_dir / "_state" / "runs.jsonl").exists()


