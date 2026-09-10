# pipeline

What a run is: a sequence of named steps, and the record of what each one did.

- `steps.py` — the steps as data. `DAILY` is the nightly run; `BACKFILL` is its last four
  over an explicit period range.
- `run.py` — `open_run`, `run_step`, `close_run`, `finalise`, and `run_pipeline`
  composing them.
- `dbt.py` — how dbt is invoked here, stated once. Every caller comes through it,
  including the test suite.
- `__main__.py` — `python -m pipeline daily`, `python -m pipeline backfill --periods
  FROM:TO`.

**This is where the orchestration logic lives, not `dags/`.** The order of the steps, the
rule that a failure stops what follows, what each step records, and the fact that
`clear-affected` runs last are facts about this platform rather than about the scheduler
that happens to be installed. They would be the same under a different one, so they live
in a package `pytest -q` exercises directly and the DAG files call into. See
`docs/adr/0046`.

**It is not responsible for** doing the work. Every step is a thin adapter over `ingest/`
or `transform/`, which own what actually happens; this package owns only the order, the
stopping, and the writing down.
