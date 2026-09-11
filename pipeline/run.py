"""Executing a sequence of steps, and writing down what each one did.

Four functions. `run_pipeline` is the three primitives composed, and the primitives are
separate because under a DAG one Airflow task is one step and the run record still has
to span all of them: the identifier is opened by the first task, carried between tasks
by XCom, and closed by a last task whose trigger rule fires however the run ended.

**The identifier is the only thing that crosses between steps.** Under a DAG each step
is its own process, so nothing else can - a SparkSession in particular cannot be handed
from one task to the next. The context therefore holds a session for the process it is
in rather than across steps, and no step stops a session it was handed. Ownership is
held here, not inferred - the rule the whole platform now follows, and the reasoning for
it is in docs/adr/0049.

See docs/adr/0044, 0045, 0046 and 0049.
"""

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from ingest import runs

__all__ = ["Context", "open_run", "run_step", "close_run", "finalise",
           "run_pipeline", "describe_failure"]


def describe_failure(failure: BaseException) -> str:
    """A failure as one line, without trusting it to describe itself.

    Formatting an exception calls its `__str__`, and one that raises there would replace
    the failure it was being asked about. Everything on this path exists to report a
    failure, so nothing on it may become one.
    """
    name = type(failure).__name__
    try:
        return f"{name}: {failure}"
    except Exception:
        return f"{name}: (its __str__ raised)"

DEFAULT_SOURCE = Path("data/source")
DEFAULT_RAW = Path("data/raw")
DEFAULT_STAGING = Path("data/staging")


@dataclass
class Context:
    """What the steps need, and nothing more.

    `landing_schema` and `mart_schema` default to what `transform.db` resolves, and are
    settable so the test suite can point a run at schemas of its own - the same reason
    `docs/adr/0034` made them configuration rather than constants.

    `spark` is the session for this process, and `owns_spark` says whether this run may
    stop it. Both default to the safe answer: no session, and not ours to stop. A run
    builds one when a step first asks and stops it at the end only when `owns_spark`
    says so - which `python -m pipeline` sets, because it is a fresh process it
    controls, and nothing else does.

    Ownership is declared rather than inferred, and that is the whole point.
    `session.build` hands back a session the process already has rather than making a
    second one, so a run that inferred ownership from having called `build` would stop a
    session belonging to somebody else. The four `__main__` blocks under `transform/`
    once inferred exactly that, from the thread-local `session.active()`; they now hold
    it with `session.acquire`, and `active` is gone. This runner keeps its own mechanism
    because it holds a session across a list of steps rather than around one command.
    See docs/adr/0049.
    """

    source_dir: Path = DEFAULT_SOURCE
    raw_dir: Path = DEFAULT_RAW
    staging_dir: Path = DEFAULT_STAGING
    periods: str | None = None
    force: bool = False
    landing_schema: str | None = None
    mart_schema: str | None = None
    spark: object = None
    owns_spark: bool = False

    # Set by the runner for the step that is running. A step reaches the run it belongs
    # to through these rather than opening a record of its own - `ingest.load` is the
    # one step that writes into the record itself, because what it has to say is
    # per-table and it is the thing that knows.
    run_id: str | None = None
    run_log: object = None

    def __post_init__(self):
        self.source_dir = Path(self.source_dir)
        self.raw_dir = Path(self.raw_dir)
        self.staging_dir = Path(self.staging_dir)
        if self.landing_schema is None or self.mart_schema is None:
            from transform import db

            values = db.settings()
            self.landing_schema = self.landing_schema or values["POSTGRES_LANDING_SCHEMA"]
            self.mart_schema = self.mart_schema or values["POSTGRES_MART_SCHEMA"]

    def log(self) -> runs.RunLog:
        return runs.RunLog(self.raw_dir)


def spark_session(context: Context):
    """The session for this process, built on first use.

    Lazily, because most runs of most step lists never touch Spark and starting a JVM
    to find that out is a cost with nothing bought. `steps.py` reaches this through the
    context; nothing else should.
    """
    if context.spark is None:
        from transform.spark import session

        context.spark = session.build("fin-pipeline")
    return context.spark


@contextmanager
def session_for(context: Context):
    """Hold the session for the length of a run, and stop it only if we own it."""
    try:
        yield
    finally:
        if context.owns_spark and context.spark is not None:
            spark, context.spark = context.spark, None
            try:
                spark.stop()
            except Exception:
                # Teardown, after the run has already recorded what it did. A session
                # that will not stop is worth neither losing that record over nor
                # masking the failure that is on its way out of this block.
                pass


# --- the primitives --------------------------------------------------------

def open_run(context: Context, *, command: str, steps,
             run_id: str | None = None,
             orchestrator: str | None = None,
             orchestrator_run_id: str | None = None) -> str:
    """Announce a run before it does anything, and hand back its identifier.

    The identifier is the platform's own, in the shape `docs/adr/0019` fixed: it is what
    `docs/adr/0018` stamps on rows, and a run started by hand has no orchestrator to
    borrow one from. An orchestrator's identifier is recorded beside it rather than
    instead of it. See docs/adr/0045.
    """
    run_id = run_id or runs.new_run_id()
    context.log().start(
        run_id,
        command=command,
        source=str(context.source_dir),
        raw=str(context.raw_dir),
        tables=[],
        steps=[step if isinstance(step, str) else step.name for step in steps],
        orchestrator=orchestrator,
        orchestrator_run_id=orchestrator_run_id,
    )
    return run_id


def run_step(context: Context, run_id: str, step) -> dict:
    """One step, with its pair of events around it.

    The started event goes down before the step runs, so a process that dies leaves the
    step it died in named by the absence of the finished one. The failure is recorded
    and then re-raised unchanged: what an exception means is the caller's to decide, and
    recording it must not change that. See docs/adr/0044.
    """
    log = context.log()
    log.step_started(run_id, step.name)

    context.run_id = run_id
    context.run_log = log
    started = time.monotonic()
    try:
        detail = step.run(context) or {}
    except Exception as failure:
        # A step that knows more about its own failure says so through `step_detail` on
        # the exception - the load's partial per-table counts, for instance, which are
        # the useful half when working out how far it got.
        elapsed = time.monotonic() - started
        try:
            detail = {"error": describe_failure(failure)}
            try:
                detail.update(getattr(failure, "step_detail", None) or {})
            except Exception:
                # Reading it can raise as easily as writing it did: `step_detail` may be
                # a property, or something that is not a mapping.
                pass
            log.step_finished(run_id, step.name, status=runs.FAILED,
                              duration_seconds=elapsed, detail=detail)
        except Exception:
            # The whole of recording, guarded. Writing the record can fail too - a
            # detail that will not serialise, a full disk - and on this path something
            # has already gone wrong. A run that reported the failure of its own
            # bookkeeping instead of the failure it was bookkeeping would have lost the
            # only thing worth reporting. The record then says less than it might; the
            # exception still says what happened.
            pass
        raise
    finally:
        context.run_id = None
        context.run_log = None

    log.step_finished(run_id, step.name, status=runs.SUCCEEDED,
                      duration_seconds=time.monotonic() - started, detail=detail)
    return detail


def close_run(context: Context, run_id: str, *, status: str,
              duration_seconds: float = 0.0, failed_step: str | None = None,
              error: str | None = None) -> None:
    """The outcome. One per run: a second would silently replace the first, and which
    of the two is true is not something this can decide."""
    log = context.log()
    record = next((one for one in log.read() if one.run_id == run_id), None)
    if record is None:
        # Appending a `finished` for a run with no `started` would make the whole log
        # unreadable from here on - `read` refuses it, and rightly. Refusing to write it
        # keeps the failure where the mistake is.
        raise runs.RunLogError(f"no run {run_id} to close")
    if record.finished_at is not None:
        raise runs.RunLogError(
            f"run {run_id} has already finished; a run writes one outcome"
        )
    log.finish(run_id, status=status, duration_seconds=duration_seconds,
               tables=[], failed_step=failed_step, error=error)


def finalise(context: Context, run_id: str) -> None:
    """Close a run however it ended, for a caller that only knows it is over.

    This is what an orchestrator's last task calls: it runs whatever happened upstream,
    so it cannot know whether the run worked. A run already closed is left alone, and
    one that never reached its last step is recorded as failed, naming the step it was
    in - a run left to read `interrupted` because nothing closed it would be
    indistinguishable from one still going, which is the reading this whole package
    exists to make precise.
    """
    record = next(
        (one for one in context.log().read() if one.run_id == run_id), None)
    if record is None:
        raise runs.RunLogError(f"no run {run_id} to close")
    if record.finished_at is not None:
        return

    # Only the latest attempt of each step counts. An orchestrator that retried a task
    # and succeeded on the second try ran a step that worked, and reading the failed
    # first attempt as the run's outcome would report every retried run as a failure -
    # which, with retries on, is an ordinary night rather than an edge case.
    latest: dict[str, runs.StepRun] = {}
    for step in record.steps:
        latest[step.step] = step

    # What the run said it would do, falling back to what it actually recorded for a
    # run written before `started` carried a step list.
    declared = list(record.requested_steps) or list(latest)
    if not declared:
        # Nothing was declared and nothing ran. That is not a success: it is a run that
        # opened and then did nothing, and calling it succeeded would put a green record
        # against a night on which no data moved.
        close_run(context, run_id, status=runs.FAILED,
                  error="the run recorded no steps")
        return

    for name in declared:
        step = latest.get(name)
        if step is None:
            close_run(context, run_id, status=runs.FAILED, failed_step=name,
                      error=f"the run never reached {name}")
            return
        if step.status != runs.SUCCEEDED:
            close_run(context, run_id, status=runs.FAILED, failed_step=name,
                      error=step.detail.get("error")
                      or f"{name} did not succeed ({step.status})")
            return

    close_run(context, run_id, status=runs.SUCCEEDED)


# --- the three composed ----------------------------------------------------

def run_pipeline(context: Context, *, command: str, steps,
                 run_id: str | None = None,
                 orchestrator: str | None = None,
                 orchestrator_run_id: str | None = None) -> str:
    """Every step in order, stopping at the first failure. Returns the run identifier.

    The failure is re-raised after it is recorded, so the caller's exit code is decided
    where it was before. Steps after the failure do not run: a mart built from a load
    that did not finish is worse than no mart, which is the whole argument for stopping.
    """
    run_id = open_run(context, command=command, steps=steps, run_id=run_id,
                      orchestrator=orchestrator,
                      orchestrator_run_id=orchestrator_run_id)
    started = time.monotonic()

    # The outcome is written inside the session's scope, so that a SparkSession that
    # throws on the way down cannot leave a run that worked reading as `interrupted`.
    # Stopping the session is teardown; what the run did is already established.
    with session_for(context):
        for step in steps:
            try:
                run_step(context, run_id, step)
            except Exception as failure:
                close_run(context, run_id, status=runs.FAILED,
                          duration_seconds=time.monotonic() - started,
                          failed_step=step.name,
                          error=describe_failure(failure))
                raise

        close_run(context, run_id, status=runs.SUCCEEDED,
                  duration_seconds=time.monotonic() - started)
    return run_id
