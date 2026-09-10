# The affected-period set is recorded by ingest and interpreted by transform

summary: Ingest writes what it observed — the periods it merged, and the dimension
versions it inserted — to `data/raw/_state/affected_periods.json`; turning a dimension
version into a set of periods is transform's job, because only transform knows the
validity intervals and which periods carry entries on that key.

## Context

`docs/adr/0016` groups an incoming batch by accounting period and merges only the
periods it touches. `ingest.load.load_table` therefore already knows exactly which
periods a run dirtied, and `ingest.raw.merge_table` already knows which rows genuinely
changed — it counts an update only when a declared column actually differs, so a nightly
extract re-presenting history it has already landed reports nothing. Both discard that
identity and keep only `partitions_written`, a count.

`docs/adr/0027` recorded the other half of the problem and left it open: a dimension
change produces no entry, so under a mechanism driven by entries' accounting dates it
triggers no recomputation at all. All three effective-dated tables —
`dim_account_src`, `dim_cost_center_src` and `fx_rate` — declare `rows_are_immutable`,
so a change to any of them can only ever be the insert of a new
`(natural key, effective_date)` version. There is nothing to diff.

Ingest and transform are separate processes. Whatever the first learns has to survive
to the second.

## Decision

`ingest/affected.py` owns `data/raw/_state/affected_periods.json`, written the way
`watermarks.json` is written — to a temporary file, then moved. It holds two lists:

```json
{
  "periods": ["2026-03"],
  "dimension_versions": [
    {"table": "dim_account_src", "key": ["6100"], "effective_date": "2026-02-01"}
  ]
}
```

`periods` are the accounting periods a merge wrote. `dimension_versions` are the
`(natural key, effective_date)` pairs a merge inserted into an effective-dated table.
Both are unioned into whatever the file already holds, never replaced: a transform may
not have run since the last ingest, and a set that forgot the run before it would
leave those periods stale for good.

**Ingest records observations. It does not turn a dimension version into periods.**
That translation needs the version's validity interval, which is a property of the
staged SCD2 dimension, and an intersection with the periods that actually carry entries
on that natural key, which is a property of the staging fact. Both belong to
`transform/`, and `transform/spark/affected.py` is what performs it.

**A period is recorded as its partition lands, and always before the watermark
moves.** Not accumulated and written once at the end of the loop. A run that dies after
writing March and before writing April has already changed March; the retry re-reads its
window, finds March already correct, and records nothing - so the only signal that
staging still owes March would be gone. Each unit of work marks itself owed as soon as
it has actually happened, which is the same shape as the watermark moving last.

The cost is one small file rewritten per dirty partition rather than one per table. The
number of dirty partitions in a run is bounded by the watermark window, not by the
ledger.

**What counts as changed is stricter than what counts as updated.** `merge_partition`
reports an update for any key the batch touched that was already present, because that
is what its number has always meant and what the summary line says. The affected set
needs the stricter question - did a declared column actually differ - which is the rule
`raw.merge_table` already applies. The two are kept apart rather than reconciled, so
that landing this did not change a number the CLI has been printing.

**The orchestrator clears the file, not the modules that consume it.** `python -m
ingest.affected --clear` is a step of its own. Neither `facts` nor `balances` clears
what it has read.

## Reasoning

Recording rather than recomputing is the cheap half. The merge has the period in hand
at the moment it writes the partition; deriving the same set afterwards would mean
re-reading the raw layer to ask a question the run had already answered and thrown away.

Keeping ingest out of the translation is what stops the layering inverting.
`docs/architecture.md` is emphatic that a contract does not import the generator and
that each layer states what it expects rather than deriving it from its neighbour. An
ingest that had to know how the SCD2 loader builds validity intervals, and which
periods a fact table holds, would be reaching two layers downstream to answer a
question it does not have the data for.

Union-on-write, and writing before the watermark, are the same convergence argument as
`docs/adr/0016`. The watermark moves only after every partition is written, so an
interrupted run re-reads its window and converges; the set is written inside that
guarantee rather than after it, so a crash re-records the same periods on the next run
and the union makes the repetition free. Every ordering here fails towards recording a
period twice rather than towards missing one, which is the direction where the cost is
a redundant recomputation instead of a figure that stays wrong.

Clearing from the orchestrator rather than from `balances` is the part that was
genuinely a choice. `balances` runs last, so clearing there would work in the ordinary
sequence and is one fewer step. It is not done because a module that silently clears
shared state on success is a module that cannot be run twice, or alone, without
consequences that are not visible where the command is typed — and running one stage by
hand is how this repository is meant to be exercised. The explicit step also gives the
DAG in `orchestrate-the-daily-run` an obvious place to hang the boundary of a run.

One thing this does not give: nothing enforces that the file is cleared. A pipeline
that never clears recomputes a growing set of periods, which is slow and correct — the
failure mode is cost, not a wrong number, and that is the direction to fail in.
