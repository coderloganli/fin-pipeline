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

Staging is written with `mode("overwrite")`. It is rebuilt from the raw layer on every
run, and holds nothing that cannot be recomputed from it.

Models land at `data/staging/<model>/`, unpartitioned, and the file names inside are
Spark's.

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

Unpartitioned, because these dimensions are tens of rows. `fct_gl_entry` is a
different question and belongs to the ticket that builds it.
