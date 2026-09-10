"""The runner: what a run is, and what it writes down about itself.

A run is a sequence of named steps. `pipeline/steps.py` declares them and
`pipeline/run.py` executes them, opening the run record before the first one and
closing it after the last however it ended. The three primitives - `open_run`,
`run_step`, `close_run` - are separate rather than folded into `run_pipeline` because
under a DAG one Airflow task is one step, and the record still has to span all of them.
See docs/adr/0046.

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
        staging_dir=tmp_path / "staging",
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
    """Case 16. The person with the problem is holding a red task in the Airflow UI;
    one field takes them to the record, and the same field takes them back."""
    run_id = runner.open_run(
        context, command="daily", steps=step_list.DAILY,
        orchestrator="airflow",
        orchestrator_run_id="manual__2026-09-08T03:00:00+00:00",
    )
    runner.close_run(context, run_id, status="succeeded")

    assert runs.main(["--raw", str(raw_dir), "--run", run_id]) == 0
    printed = capsys.readouterr().out
    assert "airflow" in printed
    assert "manual__2026-09-08T03:00:00+00:00" in printed


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


# --- closing a run somebody else was running -------------------------------
#
# `finalise` is what an orchestrator's last task calls. It runs whatever happened
# upstream, so it cannot know whether the run worked and has to read that off the
# record. See docs/adr/0046.

def test_finalise_closes_a_run_that_reached_its_last_step(raw_dir, context):
    """A run whose steps all succeeded but which nothing closed - the shape a DAG
    leaves, where `close-run` is a task of its own."""
    done = []
    plan = [recorder(name, done) for name in ("first", "second")]
    run_id = runner.open_run(context, command="daily", steps=plan)
    for step in plan:
        runner.run_step(context, run_id, step)

    runner.finalise(context, run_id)

    record = runs.RunLog(raw_dir).read()[0]
    assert record.status == "succeeded"
    assert record.failed_step is None


def test_finalise_names_the_step_a_run_never_reached(raw_dir, context):
    """Stopping early is a failure even though no step raised: the steps the run said
    it would take are on the `started` event, so the record can tell."""
    done = []
    plan = [recorder(name, done) for name in ("first", "second")]
    run_id = runner.open_run(context, command="daily", steps=plan)
    runner.run_step(context, run_id, plan[0])

    runner.finalise(context, run_id)

    record = runs.RunLog(raw_dir).read()[0]
    assert record.status == "failed"
    assert record.failed_step == "second"


def test_a_step_that_succeeded_on_a_retry_does_not_fail_the_run(raw_dir, context):
    """Airflow retries one task, not the run. A retried task that then worked ran a
    step that worked, and reading the failed first attempt as the run's outcome would
    report every retried run as a failure - which, with retries on, is an ordinary
    night rather than an edge case."""
    done = []
    flaky = recorder("first", done, fails=True)
    run_id = runner.open_run(context, command="daily",
                             steps=[flaky, recorder("second", done)])
    with pytest.raises(RuntimeError):
        runner.run_step(context, run_id, flaky)
    runner.run_step(context, run_id, recorder("first", done))
    runner.run_step(context, run_id, recorder("second", done))

    runner.finalise(context, run_id)

    record = runs.RunLog(raw_dir).read()[0]
    assert [(s.step, s.attempt, s.status) for s in record.steps] == [
        ("first", 1, "failed"), ("first", 2, "succeeded"), ("second", 1, "succeeded"),
    ]
    assert record.status == "succeeded"


def test_finalise_leaves_a_run_that_already_closed_itself(raw_dir, context):
    """`run_pipeline` closed it. The DAG's close task still fires - that is what
    ALL_DONE means - and must not write a second outcome."""
    done = []
    plan = [recorder("only", done)]
    run_id = runner.run_pipeline(context, command="daily", steps=plan)

    runner.finalise(context, run_id)

    events = [event["event"] for event in read_events(raw_dir)]
    assert events.count("finished") == 1


def test_closing_a_run_that_never_started_is_refused(raw_dir, context):
    """Appending a `finished` for a run with no `started` would make the whole log
    unreadable from that line on. Refusing keeps the failure where the mistake is."""
    with pytest.raises(runs.RunLogError, match="no run"):
        runner.close_run(context, runs.new_run_id(), status="succeeded")

    assert not (raw_dir / "_state" / "runs.jsonl").exists()


def test_the_run_is_closed_before_the_session_is_torn_down(raw_dir, context):
    """A SparkSession that throws on the way down must not turn a run that worked into
    one that reads `interrupted`. Stopping it is teardown; what the run did is already
    established by then."""
    class Unstoppable:
        def stop(self):
            raise RuntimeError("the JVM is not listening")

    context.spark = Unstoppable()
    context.owns_spark = True
    done = []

    runner.run_pipeline(context, command="daily", steps=[recorder("only", done)])

    record = runs.RunLog(raw_dir).read()[0]
    assert record.status == "succeeded"


def test_a_borrowed_session_is_not_stopped(raw_dir, context):
    """Ownership is held, not inferred. A caller that handed its session in keeps it."""
    class Watched:
        stopped = False

        def stop(self):
            self.stopped = True

    context.spark = Watched()
    context.owns_spark = False
    done = []

    runner.run_pipeline(context, command="daily", steps=[recorder("only", done)])

    assert context.spark.stopped is False


def test_finalise_refuses_to_call_an_empty_run_a_success(raw_dir, context):
    """A run that opened and then did nothing is not a success. Calling it one would
    put a green record against a night on which no data moved."""
    run_id = runner.open_run(context, command="daily", steps=[])

    runner.finalise(context, run_id)

    record = runs.RunLog(raw_dir).read()[0]
    assert record.status == "failed"
    assert "no steps" in record.error


def test_a_step_detail_that_will_not_be_read_does_not_replace_the_failure(
        raw_dir, context):
    """The runner reads `step_detail` off a failure to record what the step managed to
    do. Reading it can raise as easily as writing it can - it may be a property, or not
    a mapping at all - and a run that reported that instead of the step's own exception
    would have lost the thing worth reporting."""
    class Awkward(RuntimeError):
        @property
        def step_detail(self):
            raise ValueError("this detail cannot be read")

    def go(context):
        raise Awkward("the step itself went wrong")

    plan = [step_list.Step(name="awkward", run=go)]
    run_id = runner.open_run(context, command="daily", steps=plan)

    with pytest.raises(Awkward, match="the step itself went wrong"):
        runner.run_step(context, run_id, plan[0])

    record = runs.RunLog(raw_dir).read()[0]
    assert record.steps[0].status == "failed"
    assert "the step itself went wrong" in record.steps[0].detail["error"]


def test_a_failure_that_cannot_describe_itself_still_propagates(raw_dir, context):
    """Formatting an exception calls its `__str__`. One that raises there would replace
    the failure it was being asked about - so nothing on the path that reports a failure
    is allowed to become one."""
    class Mute(RuntimeError):
        def __str__(self):
            raise ValueError("cannot say")

    def go(context):
        raise Mute()

    plan = [step_list.Step(name="mute", run=go)]
    run_id = runner.open_run(context, command="daily", steps=plan)

    with pytest.raises(Mute):
        runner.run_step(context, run_id, plan[0])

    record = runs.RunLog(raw_dir).read()[0]
    assert record.steps[0].status == "failed"
    assert "Mute" in record.steps[0].detail["error"]


def test_a_detail_that_will_not_serialise_does_not_replace_the_failure(
        raw_dir, context):
    """Writing the record can fail too. A run that reported the failure of its own
    bookkeeping instead of the failure it was bookkeeping would have lost the only thing
    worth reporting."""
    class Unwritable(RuntimeError):
        step_detail = {"partitions": {object()}}  # a set of an object: not JSON

    def go(context):
        raise Unwritable("the step itself went wrong")

    plan = [step_list.Step(name="unwritable", run=go)]
    run_id = runner.open_run(context, command="daily", steps=plan)

    with pytest.raises(Unwritable, match="the step itself went wrong"):
        runner.run_step(context, run_id, plan[0])
