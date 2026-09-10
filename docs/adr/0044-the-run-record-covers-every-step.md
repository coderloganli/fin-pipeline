# The run record covers every step, not only the load

summary: `runs.jsonl` gains a `step_started`/`step_finished` pair per step inside the
run's own pair, so a run is a sequence of named steps and the record says which one it
died in.

## Context

`docs/adr/0019` established the run record: two lines per run, appended and never
rewritten, so a run that dies leaves its first line and is reported as `interrupted`.
It was written when a run meant one thing — `ingest.load`.

A run has since come to mean seven commands. Validation, the load, the SCD2 build, the
point-in-time fact, the monthly balances, the copy into Postgres and `dbt build` are
typed in order by whoever is running the platform, and only the second of them writes
anything down. So the question the record exists to answer — what happened last night —
is answerable for one seventh of the run and is otherwise a matter of who still has the
terminal open.

That is the failure this ticket was opened for. Asked which part of running the pipeline
hurt most, the answer was not the typing. It was finishing a run and not knowing what
came of it.

An orchestrator does keep its own history, and leaning on it was the obvious
alternative. It is rejected below.

## Decision

The log keeps its shape and gains a level. A run writes its `started` event, then a
pair of events around each step, then its `finished` event:

    {"event": "started",       "run_id": R, "command": "daily", "steps": [...]}
    {"event": "step_started",  "run_id": R, "step": "validate", "started_at": ...}
    {"event": "step_finished", "run_id": R, "step": "validate", "status": "succeeded",
                               "duration_seconds": ..., "detail": {...}}
    ...
    {"event": "finished",      "run_id": R, "status": "failed",
                               "failed_step": "facts", "error": ...}

`Run` gains `steps` and `failed_step`. `StepRun` carries the step's name, status,
duration, and a `detail` object holding whatever that step has to say — for `load`, the
per-table counts, watermark range and source digest that sit on the `finished` event
today.

The consistency rules `RunLog.read` already applies to runs apply to steps, with one
distinction runs do not have to make. A step that finishes without having started raises
`RunLogError`, as a run does. A step that starts again **after its previous attempt
finished** is a retry, and is folded as a second attempt of that step rather than as an
error — an orchestrator retrying one task is an ordinary event, and a record that
refused to read it would be refusing to describe the night it is most needed for. A step
that starts again while its previous attempt is still open is the concatenated-or-edited
log `docs/adr/0019` refuses to read, and still raises.

`StepRun` therefore carries an `attempt` number, and a run's `steps` list holds every
attempt in log order. A step with a `step_started` and no `step_finished` is where the
run stopped, and `read` reports it.

**Logs written in the two-event shape still read**, folding to a run with no steps. The
file is appended to and never rewritten, so a log spanning this change is the ordinary
case rather than something to migrate.

## Reasoning

Append-only is what made the crash case work in `docs/adr/0019`, and it is what makes
this work: the step events are the same trick applied one level down. What is written
first is what survives the failure, so the run that died at `facts` leaves a
`step_started` for `facts` and nothing after it. That is a more precise statement than
`interrupted` and it is free — it is the absence of a line, not an extra one.

Nesting rather than a second log keeps one file answering one question. Two files would
have to be joined by hand at the moment somebody is trying to find out why the mart is
short, and either could be the one that is missing.

Leaning on the orchestrator's history instead was rejected for the reason
`docs/adr/0019` put the record in a file rather than in Postgres: it has to be readable
when the thing that broke is a service. An Airflow deployment that will not start is
exactly when the question gets asked, and a record inside it is a record you cannot
reach. It would also split the history — a run started by hand would be in one place and
a scheduled run in another — which makes "show me the last ten runs" a question with two
answers.

The cost is that the log grows by two lines per step rather than two per run: fourteen
lines a night instead of two. `docs/adr/0019` measured the old rate at under a megabyte
a decade, so this is under a megabyte a year, and a rotation policy would still be code
with no reader.

The `detail` object is deliberately unstructured. Each step has something different
worth recording, and a schema covering all of them would either be mostly empty columns
or would have to be revised by every task that adds a step. What is structured is what
every step shares: its name, whether it worked, and how long it took.
