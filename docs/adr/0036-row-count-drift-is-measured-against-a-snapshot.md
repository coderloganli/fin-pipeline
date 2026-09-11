# Row-count drift is measured against a snapshot the mart keeps itself

summary: Every dbt build appends each model's row count to `mart.model_row_count`; the
drift gate compares the current count with the median of the previous five builds,
excluding the build being tested, and fails outside ±10%.

## Context

One of the six gates is row-count drift, and drift is by definition a comparison with
something earlier. The other five gates read only the tables in front of them. This one
needs a baseline, and where the baseline is kept decides whether the gate can be
trusted.

## Decision

`mart.model_row_count` is an incremental dbt model with one row per counted model per
build: `(invocation_id, built_at, model, row_count)`. `invocation_id` is dbt's own, so a
build identifies itself without a second identifier being invented.

It depends on every model it counts and it does not count itself. That is what keeps
the graph acyclic: the models build, then `model_row_count` appends this build's row,
then the drift test — a singular test on `model_row_count` — reads both the current row
and the history from the same table. A model that counted its own output in the same
invocation would depend on itself, and excluding it costs nothing, because the number of
rows in a table of row counts is not a figure anyone reports.

Two numbers, both declared in `dbt_project.yml` as vars so a scenario can move them:

- `drift_window: 5` — the number of previous builds the baseline is taken over.
- `drift_tolerance: 0.10` — the gate fails when the current count divided by the median
  falls outside `[0.90, 1.10]`.

Below `drift_window` previous builds the gate passes and says it is still filling its
baseline — a gate with one prior observation would fire on the second build of a fresh
clone.

`mart.model_row_count` is excluded by name from the reproducibility check. The
acceptance criterion is that three runs leave every mart table's row count and checksum
unchanged; a table whose whole purpose is to accumulate one row per run cannot also be
invariant across runs, and pretending otherwise would mean either a broken criterion or
a gate that keeps no history.

## Reasoning

The median rather than the mean, because one legitimately large backfill should not
raise the baseline enough to hide the shrink that follows it.

Five builds and ten percent are the starting values, not derived ones, and they are vars
rather than constants for that reason. Five is the smallest window over which a median
ignores a single outlier. Ten percent is wide enough that a day of new entries in a
monthly-grain mart does not fire it and narrow enough to catch a load that lost a
partition, which is the failure this gate is for. Moving them is a decision to record
here, not a knob to turn quietly.

Excluding the current build is not a detail. Included, the current count contributes to
the median it is being compared against, which drags the baseline toward whatever
happened and weakens the gate exactly when it should fire hardest — a single run that
halves every table.

`data/raw/_state/runs.jsonl` was the alternative baseline and it was declined. It
records source row counts, which are not model row counts: the dense monthly grid of
`docs/adr/0033` has a row count driven by the period range rather than by activity, and
an aggregate has fewer rows than its input by construction. A gate reading source
counts would be comparing two different quantities and calling the difference drift.

A fixed threshold — non-empty, under some written-in ceiling — was declined because it
is not a drift gate. It catches a table that collapsed to nothing and misses a table
that grows ten percent a run until the figures are wrong.

## Consequences

The gate is quiet on a fresh clone until the baseline fills, and the test suite
therefore seeds the snapshot table rather than waiting for real builds. The seeding is
part of the gate's own test: a scenario that plants a history and then loads half the
ledger is what proves the gate can turn red.

**This gate runs last, and a failure above it stops it running at all.** `dbt build`
skips a model's descendants when one of its tests fails, and `model_row_count` descends
from every model it counts. So a run where `fct_gl_entry` fails its uniqueness or
balance test never reaches the drift gate. That is the right order — measuring drift on
a table that has already failed its own tests would report a second symptom of one
cause — but it constrains how the gate can be tested: the scenario that halves the
ledger has to leave every other gate green, or it is exercising the skip and not the
drift. Deleting whole accounting periods does that, because both lines of a voucher
carry the same accounting date; deleting a random sample of rows does not.

**The baseline is a history of promoted builds.** `docs/adr/0048` builds the mart into a
schema of its own and renames it into place on success, and `model_row_count` is copied
into that schema and swapped with it. So a build that appends its row and then fails a
gate takes that row away with the schema it was discarded in. The window of five is five
builds whose figures were accepted, which is what a baseline should be made of — a
rejected build setting the expectation for the next one would be the gate arguing with
itself.
