"""Executing a sequence of steps, and writing down what each one did.

`run_pipeline` is the three primitives composed, and they stay separate because a
caller may want to drive the steps itself: the identifier is opened once, carried
through, and closed after the last step however it ended. The scheduler here is a cron
entry against `python -m pipeline daily`, and it can be one because nothing about a run
lives in the scheduler - see docs/adr/0046.

**The identifier is the only thing that has to cross between steps.** Everything else
belongs to the process. Nothing here holds an engine: the transformations are SQL that
dbt runs against the database, so a step needs a connection and not a session.

See docs/adr/0044 and 0045.
"""

import time
from dataclasses import dataclass, field
from pathlib import Path

from ingest import runs

__all__ = ["Context", "open_run", "run_step", "close_run",
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


@dataclass
class Context:
    """What the steps need, and nothing more.

    `landing_schema` and `mart_schema` default to what `transform.db` resolves, and are
    settable so the test suite can point a run at schemas of its own - the same reason
    `docs/adr/0034` made them configuration rather than constants.

    """

    source_dir: Path = DEFAULT_SOURCE
    raw_dir: Path = DEFAULT_RAW
    periods: str | None = None
    force: bool = False
    landing_schema: str | None = None
    mart_schema: str | None = None
    anomaly_schema: str | None = None

    # Set by the runner for the step that is running. A step reaches the run it belongs
    # to through these rather than opening a record of its own - `ingest.load` is the
    # one step that writes into the record itself, because what it has to say is
    # per-table and it is the thing that knows.
    run_id: str | None = None
    run_log: object = None

    def __post_init__(self):
        self.source_dir = Path(self.source_dir)
        self.raw_dir = Path(self.raw_dir)
        if (self.landing_schema is None or self.mart_schema is None
                or self.anomaly_schema is None):
            from transform import db

            values = db.settings()
            self.landing_schema = self.landing_schema or values["POSTGRES_LANDING_SCHEMA"]
            self.mart_schema = self.mart_schema or values["POSTGRES_MART_SCHEMA"]
            # Resolved here rather than reached for inside the step, so a test that
            # isolates a run by passing schemas can isolate this one too. A resolver
            # default alone would have every pipeline test writing into whatever
            # `anomaly` resolves to in the developer's own database.
            self.anomaly_schema = self.anomaly_schema or values["POSTGRES_ANOMALY_SCHEMA"]

    def log(self) -> runs.RunLog:
        return runs.RunLog(self.raw_dir)



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
