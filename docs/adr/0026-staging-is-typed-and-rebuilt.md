# Staging is typed, and it is rebuilt rather than accumulated

## Context

`docs/adr/0015` lands every raw column as text, because the raw layer exists to answer
whether the source really said that, and a layer that has already reinterpreted cannot
answer it. `docs/adr/0023` makes the raw layer keep every primary key it has ever
landed, because once an extract has landed the warehouse is the only party that can
still say what the source said.

Staging is the first layer that is allowed to interpret. This record says what that
means for its types and for what happens to it on a rerun.

## Decision

Staging is Parquet with real types. `valid_from` and `valid_to` are dates,
`is_current` is a boolean, and the natural key and attribute columns stay strings
because that is what they are.

Staging holds nothing that cannot be recomputed from the raw layer.

**The dimensions are rebuilt whole.** `dim_account`, `dim_cost_center`, `dim_fx_rate`
are written with `mode("overwrite")` at `data/staging/<model>/`, unpartitioned, and the
file names inside are Spark's.

**The fact tables and the aggregate are partitioned by accounting period, and a run
rewrites only the partitions it has reason to.** `fct_gl_entry`, `fct_gl_adjustment` and
`agg_monthly_balance` land at
`data/staging/<model>/accounting_period=YYYY-MM/`, matching the raw layer's layout. A
run with no dirty set rewrites every partition and is the same thing as the overwrite
this record originally specified. A partition is written by overwriting its own
directory rather than by writing the table with `partitionBy`, because Spark's default
`partitionOverwriteMode` of `STATIC` replaces the whole table directory. See
`docs/adr/0041`.

## Reasoning

Retyping is the job the architecture assigns to this layer, and it has to happen
somewhere. Leaving `valid_from` a string would push a cast to every consumer of a
range join, and the consumer that gets the cast wrong produces wrong attribution
rather than an error - a string comparison between `'2026-04-01'` and `'2026-4-1'` is
false and reads as if it were a date comparison.

Overwrite is the counterpart to the raw layer's refusal to forget, not a contradiction
of it. The raw layer is the record: it accumulates because losing what it holds loses
information nothing can recover. Staging is derived: everything in it is a function of
raw, so rebuilding it is free and keeping history in it would be storing a second copy
of an answer that can be recomputed - and a second copy is a second thing that can
disagree. The rule this leaves is easy to hold: the layer that is told things
accumulates, and the layers that compute things are rebuilt.

A consequence worth stating: because staging is rebuilt, the dimension it produces
always reflects everything raw currently holds. It is not a record of what staging
looked like at a past run, and nothing downstream should read it as one.

Spark names its output files itself, and the names are not stable across runs. That
does not affect the acceptance criterion, because the checksum is over rows rather
than bytes - `docs/adr/0017` - which was decided for a related reason: a Parquet file
carries its writer's version, so byte comparison was never going to be the check. A
single output file per model comes from coalescing before the write; at these row
counts that costs nothing and keeps the layer readable by hand.

Unpartitioned, because these dimensions are tens of rows.

**The fact and the aggregate were the different question this record deferred, and the
answer is not the same one.** Overwrite is still the right default for a derived layer,
and it stays the behaviour of a run that has no dirty set to work from. What changed is
that a late entry is a normal event rather than an exceptional one, so "rebuilt" and
"rewritten whole every time" stopped being the same statement. A correction to March
recomputes March and the periods whose windows read through it, and the other partitions
are not opened - which is the property `docs/adr/0016` already gives the raw layer, at
the layer where the acceptance criterion is checked.

Overwrite's original argument survives intact underneath this: everything in staging is
still a function of raw, nothing here is a record, and a full rebuild is always
available and always produces the same answer. Selective rewriting is an optimisation
that is required to be indistinguishable from the rebuild, and
`docs/adr/0041` is what keeps it so - the dirty set scopes writes, not reads, precisely
so that a partially recomputed table cannot disagree with a fully recomputed one.

See `docs/adr/0039`, `0040` and `0041`.
