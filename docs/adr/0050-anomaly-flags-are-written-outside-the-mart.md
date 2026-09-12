# Anomaly flags are written outside the mart, by a step that runs after it is published

summary: `anomaly_flag` lives in a schema of its own, written by a `judge` pipeline step
that runs after `dbt-build` has promoted the mart, so a model that fails leaves the
reported figures published and untouched.

## Context

The anomaly model reads monthly balances and writes one row per flagged balance. Where
that row lands has to be settled before anything is written, because two mechanisms in
this repository already have opinions about the mart schema.

`docs/adr/0048` promotes the mart by renaming a schema: a build writes
`<mart>__b<run_id>` and `transform/promote.promote` does `DROP SCHEMA <mart> CASCADE`
followed by `ALTER SCHEMA ... RENAME TO`. Anything sitting in the mart schema that dbt
did not build is not in the build schema, and the next promotion deletes it.

`docs/adr/0036` gives the drift gate a memory in `mart.model_row_count`, and the gate
compares each counted model against the median of the previous five builds. A table of
anomaly flags has a row count that is supposed to move — that is what it measures — so
a gate built to catch a load that lost a partition would fire on it every month.

## Decision

**`anomaly_flag` lives in `POSTGRES_ANOMALY_SCHEMA`, default `anomaly`.** It is a
setting resolved by `transform/db.py` alongside the landing and mart schema names, for
the reason `docs/adr/0034` gives for those two: running the test suite must not
overwrite the schema a developer has been looking at in the same database.

**The table is created and written by Python, not by dbt.** It is a model output rather
than a transformation of the landing layer, and dbt has no way to produce it.

**A `judge` step joins the pipeline after `dbt-build` and before `clear-affected`.**
`DAILY` becomes seven steps and `BACKFILL` five. The step is a thin adapter over `ml/`,
the shape every other step in `pipeline/steps.py` already has, and `dags/` gains one
task in each of the two DAGs.

**`Context` carries the schema, and resolves it the way it resolves the other two.**
`pipeline/run.Context` has `landing_schema` and `mart_schema` and fills them from
`db.settings()` in `__post_init__`; it gains `anomaly_schema` beside them. A resolver
default alone would not do: `tests/test_daily.py` isolates a run by passing schemas into
the context, and a third schema not on the context would have every pipeline test
writing into whatever `anomaly` resolves to in the developer's own database.

**The step works out which periods to judge, and does not take them from the step
before.** `Context.periods` is `None` on a daily run and a range on a backfill, and
`docs/adr/0046` keeps step-to-step handoff out of the DAG on purpose. So:

- **Backfill**: judge `context.periods`, the range the caller named.
- **Daily**: read `ingest.affected.read(context.raw_dir)` and widen it with
  `transform.spark.balances.dirty_closure`, which is the same closure `recompute` used
  to decide what to rewrite. Calling that function rather than restating the rule is the
  point `docs/adr/0040` makes: a widened window must not leave a consumer behind.

This is why `judge` must run before `clear-affected` and not merely before the end of
the run — after the clear there is nothing left to read.

**The drift gate does not see it.** `model_row_count.sql` counts a written-down list of
seven dbt models; `anomaly_flag` is not a dbt model, is referenced by no `ref()`, and is
not in the mart schema. It is outside that graph rather than excused from it.

## Reasoning

**The deciding argument is not the promotion, it is what an ml failure is allowed to
do.** `docs/product.md` says of this layer that it is the one read-only leaf of the
whole graph — "delete it entirely and not a single reported number changes". A layer
with that standing must not be able to stop the mart being published. Putting the flags
inside the mart schema and inside the promotion transaction would give it exactly that
power: a model that raised would roll back the rename and leave the analyst reading last
month's figures because a regression did not converge.

Running the step after promotion follows from the same sentence. By the time `judge`
runs, the mart is published and correct; the step can fail, be reported as having
failed, and leave every reported number where it is. The queue is missing that night,
which is the correct blast radius for this layer.

**Before `clear-affected` rather than after**, for the ordering argument
`docs/adr/0046` makes about that step: what is cleared first is what goes missing when
the run dies. A run that reaches `judge` and fails still owes its affected periods.

**A schema of its own rather than a durable table inside the mart schema.** The
alternative — keep `anomaly_flag` in the mart schema and teach `promote` to carry it
across, as it already carries `model_row_count` — was considered and declined.
`model_row_count` is carried because it is the drift gate's memory and the gate is
inside the build; carrying a second table for a different reason would make the
promotion a place where unrelated exceptions accumulate. The promotion's argument is
that what is promoted is what happened, one rule and no compensating steps.

**Not a dbt model over a Python-written landing table**, which was the other shape: the
model would have to run before `dbt-build`, and `docs/adr/0051` puts its training data
in the mart, which does not exist yet at that point. The dependency is circular and the
shape does not survive it.

## Consequences

**A fourth schema, and a fourth thing to point at a test database.** `DEFAULTS` in
`transform/db.py` gains `POSTGRES_ANOMALY_SCHEMA`, `.env.example` records it,
`pipeline/run.Context` gains `anomaly_schema`, and the test fixtures point it somewhere
of their own — `schema_for(request.node.nodeid, ...)`, as they already do for the other
two, with `drop_schemas_under` tearing it down by prefix.

**A judged period's flags are replaced, not added to.** `judge` deletes the flags this
arm wrote for the periods it is about to judge and writes the new ones in the same
transaction. Append would leave a row flagged that a rebuild has since brought back
inside its interval — a queue entry for a figure that is no longer anomalous, which is
worse than a missing one because somebody spends time on it.

**Scoped to the arm, not to the period.** `model_family` is in the key so that both arms
can hold a flag for one balance, which is what makes them comparable; a delete that
ignored it would have a run with one arm silently discard the other's work.

**A period the model cannot judge is reported, not emptied.** A period with less than a
year of history in front of it produces no prediction — `docs/adr/0055` consumes twelve
periods before a series has a row — and replacing its flags anyway would delete what
stood against it and write nothing back. The step reports such periods separately from
the ones it judged, because "judged and found nothing" and "could not judge" are
different facts and a `flagged: 0` that meant either would be the kind of silence
`docs/product.md` calls a defect.

**The flags outlive the mart they were computed from.** A promotion replaces the mart
wholesale; the flags do not move. A flag therefore carries the `run_id` of the run that
produced it, which is how a reader gets back to the mart build it was computed against
through the run record — the same indirection `docs/adr/0038` uses to keep the build out
of a mart row.

**A failed `judge` step fails the run.** The mart is published by then, so the failure
means the queue was not refreshed, not that the figures are wrong. That is the opposite
trade from the sweep in `docs/adr/0048`, which is allowed to fail without failing the
run because nothing downstream of it is owed; here something is.
