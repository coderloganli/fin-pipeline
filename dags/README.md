# dags

Airflow orchestration. **Declarations only.**

- **daily** — validate, load, recompute, mart-load, dbt-build, clear-affected. Scheduled.
- **backfill** — the last four of those over an explicit period range, triggered by hand
  with `{"periods": "YYYY-MM:YYYY-MM"}`. It rebuilds that range whether or not the
  affected-period set names it, which is the recovery path `docs/adr/0016` describes.
- **evaluate** — the golden set. Not built: the golden set it would run does not exist.

Backfill is a first-class scenario here rather than an afterthought, which is why Airflow
was chosen over a simpler scheduler.

## What belongs in a file here, and what does not

The order of the steps, stop-on-failure, what each step records, and the fact that
`clear-affected` runs last all live in `pipeline/`. A file here declares a schedule, a
task graph and a trigger rule, and calls into that package. **A DAG file imports
`pipeline` and nothing below it** — `ingest`, `transform` or `generator` appearing in one
means logic has leaked back. See `docs/adr/0046`.

## The literal convention, and why it exists

`tests/test_dags.py` reads these files with `ast` and imports no Airflow, because
`docs/adr/0004` keeps `apache-airflow` off the host and this repository holds that a test
which skips when its subject is absent is not a test. An AST can only read literals, so
these files are written to be readable:

- every `task_id` is a **string literal** naming its step, exactly as `pipeline.steps`
  names it, plus `open-run` first and `close-run` last;
- the close task carries the literal `trigger_rule=TriggerRule.ALL_DONE`.

That is what catches a step added to `pipeline.steps` and not to a DAG, which would
otherwise appear as a step that quietly never ran.

## What no test here can tell you

Whether the edges connect, whether XCom carries the run identifier, whether the trigger
rule behaves as its name says, or whether Airflow will accept the file at all. Nothing on
this host runs a DAG. That is checked with a command:

```
docker compose run --rm airflow-dag-processor airflow dags list
```
