# A mart row carries the run that landed the source, not the build that shaped it

summary: `fct_gl_entry` keeps `source_first_run_id` and `source_last_run_id` and gains
no column identifying the dbt build, so rebuilding the mart from unchanged inputs
leaves every row byte-identical.

## Context

`docs/adr/0018` puts two run identifiers on every raw row, and
`transform/spark/facts.py` carries them into the staging fact. The mart is the layer an
analyst and the insight layer read, and `docs/product.md` requires every reported
figure to name the rows it came from and the run that produced them.

The obvious reading is that the mart fact should also record which dbt build wrote it.
The acceptance criterion says otherwise: the same run three times over must leave every
mart table's row count and checksum unchanged.

Those two cannot both hold on the same column. A build identifier changes every build
by definition, so a row carrying one has a different checksum every time.

## Decision

Mart rows carry `source_first_run_id` and `source_last_run_id` and nothing else about
provenance. No `dbt_invocation_id`, no `built_at`, no load timestamp on the row.

Which build produced the current state of a table is recorded once per model per build
in `mart.model_row_count` — the table `docs/adr/0036` already keeps — and is reached
from a row through the model name, not through a column on the row.

## Reasoning

The question a provenance column has to answer is "where did this figure come from",
and `source_last_run_id` answers it: it names the ingest run, and `python -m
ingest.runs` turns that into the window, the source digest and the row counts. The dbt
build that reshaped it adds nothing to that trail, because it is a pure function of its
inputs — which is precisely what the reproducibility criterion asserts and what makes
the column redundant.

This is the same shape `docs/adr/0020` settled for the source file hash: metadata that
belongs to a run is reached from the run record rather than stamped on every row.

Stamping the build on the row would also make the reproducibility check untestable in
the only form that means anything. A checksum that skips the columns that change is a
checksum over the columns nobody worried about.

## What the reproducibility check actually covers

Two readings of "the same run three times over" were both live, and they are not the
same claim:

**Three mart rebuilds over one fixed staging snapshot** — load, `dbt build`, three
times. Everything is identical, every column included. This is the property this task
owns, and it is the one the acceptance criterion is checked against.

**Three complete pipeline reruns** — generator through mart. Here `source_last_run_id`
does change, and legitimately: `docs/adr/0018` has `_last_run_id` set by whichever run
wrote the partition, so a rerun that rewrites a partition moves it on every row in that
partition. The figures do not change, and that is what the check asserts.

So the mart checksum is taken over the reported columns and excludes
`source_first_run_id` and `source_last_run_id`. This is not an exception invented here:
`docs/adr/0017` already excludes ingestion metadata from the raw checksum, for the same
reason and in the same words — a checksum that includes a column designed to change on
every write is a checksum that can never hold.

Both are tested. The first is exact over every column; the second is over the reported
columns, with the provenance columns asserted separately to be a valid run identifier
rather than asserted equal.

## Consequences

Recovering which build wrote a given table means reading `mart.model_row_count` for the
most recent row for that model. That is one lookup, and it is the same lookup the drift
gate already makes.
