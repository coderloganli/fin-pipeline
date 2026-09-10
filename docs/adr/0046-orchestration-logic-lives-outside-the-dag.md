# The orchestration logic is a Python package; the DAG is a declaration

summary: Sequencing, stop-on-failure, the run record and the clearing of the
affected-period set live in `pipeline/`, which `pytest -q` exercises directly; the files
under `dags/` declare the schedule and the task graph and import nothing below
`pipeline`.

## Context

`docs/adr/0004` keeps `apache-airflow` out of the host's dependency declaration: it
lives in its own image, and that is what dissolved the constraint that Airflow does not
run natively on Windows.

This repository also holds, in the same record, that a test which skips when its subject
is absent is not a test. Both of those are load-bearing, and orchestration is where they
collide: if the sequencing lives inside DAG files, then testing the sequencing means
either installing Airflow on the host — reopening what `docs/adr/0004` closed — or moving
part of the suite into a container, so `pytest -q` stops being the whole of it.

## Decision

`pipeline/` is an ordinary Python package. It holds:

- `steps.py` — the steps as data: a name, what it calls, and what detail it records.
  `DAILY` and `BACKFILL` are ordered lists of them.
- `run.py` — `open_run`, `run_step`, `close_run`, and `run_pipeline` composing the three.
- `__main__.py` — `python -m pipeline daily`, `python -m pipeline backfill --periods ...`.

Everything that can be wrong about a run is in there: the order, that a failure stops
what follows, what each step records, and that `clear-affected` runs last.

`dags/daily.py` and `dags/backfill.py` declare one Airflow task per step, wire them in
order, and call into `pipeline`. **A DAG file imports `pipeline` and nothing below it.**
`ingest`, `transform` or `generator` appearing in one means logic has leaked back.

`open_run`, `run_step` and `close_run` are separate rather than folded into
`run_pipeline` precisely so that one Airflow task can be one step while the run record
still spans the whole DAG run: the identifier is opened by the first task, carried by
XCom, and closed by a last task whose trigger rule fires however the run ended.

**The run identifier is the only thing that crosses between steps.** Under a DAG each
step is its own process, so nothing else can: a SparkSession in particular cannot be
handed from one task to the next. The runner therefore holds a session for the process it
is in rather than across steps — `pipeline/run.py` provides it as a context manager — and
the step modules take `spark` as an argument, which they already do. **No step stops a
session it was handed.** Under `python -m pipeline daily` that is one session for all six
steps; under a DAG it is one per task, out of the same code.

That deliberately does not reuse the `borrowed = session.active() is not None` inference
in the `__main__` blocks of `scd2`, `facts`, `balances` and `backfill`, because the
inference is unsound. `session.active()` is `SparkSession.getActiveSession()`, which is
thread-local and returns `None` when a session exists in the process but is not active in
the calling thread; `session.build()` goes through `getOrCreate`, which hands back that
existing session. `borrowed` is then `False` and the caller stops a session belonging to
someone else. Ownership has to be held, not inferred, which is why the runner holds it.

**A backfill over an explicit range ignores the affected-period set.**
`transform.backfill.run` returns without rebuilding anything when the set is empty, which
is right for the daily run and wrong for a backfill: the reason to type a range by hand
is that the set does not name it — a fixed bug, or an update that arrived after the
watermark window closed, which `docs/adr/0016` names as the case `--full` recovers from.
`run` therefore takes `force`, under which the dirty set is the requested range unioned
with whatever is owed, and `pipeline.steps.BACKFILL` passes it.

The suite tests `pipeline` directly and tests the DAG files by parsing them with `ast` —
that they name the runner's steps, in the runner's order, and that they import nothing
below `pipeline`. No test imports Airflow.

## Reasoning

The split is not a testing trick; it is where the logic belongs. The order of the seven
commands, and the rule that the affected-period set is cleared only after everything
downstream of it has been rebuilt, are facts about this platform. They would be the same
under a different scheduler, and writing them inside DAG files would make them a property
of the scheduler that happens to be installed.

What is left in a DAG file is what genuinely is Airflow's: a schedule, a task graph, a
trigger rule, retries. The `ast` test does not verify that the graph runs — nothing on
the host can, and pretending otherwise would be the skipped test this project refuses. It
verifies the one thing that can silently drift: a step added to `pipeline.steps` and not
to the DAG, or renamed in one and not the other. That failure would otherwise appear as a
step that quietly never ran.

Testing DAG structure with `dag.test()` was the alternative and it is a real facility —
it runs a DAG in one process without a scheduler. It needs Airflow installed and a
metadata database, even a local SQLite one, which is `docs/adr/0004` reopened for a test
that would still not be running the containerised path CI and the developer use. Running
part of the suite inside the Airflow container was the other alternative, and it costs
the property that `pytest -q` is the whole of the test suite — a property this repository
states in its README and in `docs/architecture.md`, and which is worth more than
coverage of a file that declares a graph.

The dbt step is the one place where "declare the step, call the module" needed something
built rather than found. `tests/conftest.py` already invokes dbt as a subprocess and
already supplies the `--project-dir`, `--profiles-dir` and schema environment that
invocation needs, and `pipeline/` cannot import a helper that lives under `tests/`. The
invocation moves to `pipeline/dbt.py` and the fixture calls it, so there is one statement
of how dbt is run here rather than two that can drift.

The honest limit is stated rather than hidden. **Nothing in the suite proves the DAGs
parse under Airflow, and nothing proves the graph they declare is wired the way it
reads.** The `ast` test reaches what is written in the file — that there is a task per
step, in the runner's order, that `open-run` and `close-run` are there, that the close
task carries a trigger rule that fires on failure, and that nothing below `pipeline` is
imported. It cannot reach whether the edges connect, whether XCom carries the identifier,
or whether Airflow will accept the file at all. `docker compose run --rm
airflow-dag-processor airflow dags list` is what reaches those, it is a documented command
rather than a test, and a DAG file thin enough to hold no logic is a DAG file with little
left to get wrong.
