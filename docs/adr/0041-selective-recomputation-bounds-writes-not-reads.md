# Selective recomputation bounds what is written, not what is read

summary: A backfill rewrites only the dirty staging partitions, but still reads the
whole fact table for the two columns that decide the dense grid and the account type;
the acceptance criterion is about modification times, and reading is columnar.

## Context

`docs/adr/0033` makes the monthly grid dense: every active combination of account and
cost centre carries a row for every period, zero where nothing posted, because a
period-over-period comparison over a sparse table silently becomes a comparison with
the last month that had activity.

That grid is built from `entries.select("account_code", "cost_center_code").distinct()`
— the combinations that have activity *anywhere*. And `docs/adr/0032`'s account type is
carried into periods that posted nothing by a window that reaches both backwards and
forwards over every period the combination has.

Both of those are properties of the whole fact table. A backfill that read only the
dirty periods would compute a narrower identity set and a different type fill, and would
write rows for the dirty periods that disagree with the rows around them — a dense grid
that lost members, which is exactly the failure `docs/adr/0033` exists to prevent.

## Decision

The dirty set scopes what is **written**. It does not scope what is read.

`facts.build` reads only the raw partitions it is rebuilding, because an entry's
attribution is a function of that entry and the dimensions, and nothing else.

`balances.build` reads the whole staging fact and computes the whole aggregate. Then it
writes only the partitions in the dirty closure of `docs/adr/0040`.

That is the honest statement, and it is less clever than scoping the read would be. It
is what the model requires: the grid's membership, the type fill and the window columns
each need periods outside the closure, so the set of periods that must be *read* is not
meaningfully smaller than all of them, and computing a subset would mean computing a
different answer for the rows at its edges. The saving this ticket delivers at the
aggregate is the write, not the read.

**A partition is written by writing its directory, not by writing the table with
`partitionBy`.** Each dirty period is filtered out, the partition column dropped, and
written with `mode("overwrite")` to `data/staging/<model>/accounting_period=<P>/`. Spark
then replaces one directory and never opens the others.

**A change in the grid's membership dirties every period.** If the set of
`(account_code, cost_center_code)` combinations differs from the one the aggregate
currently holds, the closure is discarded and every period in the reporting range is
rewritten.

## Reasoning

The saving is real where it is claimed and nowhere else, which is worth being exact
about. `facts.build` genuinely reads less: an entry's attribution is a function of that
entry and the dimensions, so it opens only the raw partitions it is rebuilding. The
aggregate reads everything and writes little. Both are bounded by the same rule - the
dirty set scopes writes - and only one of them also scopes reads.

The alternative was worse than the cost, which is the same shape of argument
`docs/adr/0016` makes for the key sweep. There the alternative was a raw layer holding
two rows for one primary key; here it is a dense grid with holes in it, which is
quieter. Recomputing an aggregate over a monthly grid is cheap; a report missing rows
is not.

The acceptance criterion is about modification times — an untouched partition's file is
not rewritten — and a read does not change one. Stating the rule as "writes are scoped"
rather than "the backfill only touches the dirty periods" is more precise and it is
what the code actually does, which is the more useful thing for the criterion to be
tested against.

The cost is stated rather than hidden, as `docs/adr/0011` and `0016` do: a backfill of
one period still recomputes the whole aggregate. That is bounded by the fact table and
the number of periods, not by how little changed, and at a monthly grain over a few
years it is small. If it ever stops being small, the identity set and the type fill are
both derivable from a summary the aggregate could keep for itself — which is a table
that can disagree with the fact it summarises, and therefore not worth building before
the read is actually a problem.

The computed result is cached before the partitions are written, because writing N
partitions off an uncached plan would recompute that whole aggregate N times.

Writing one directory at a time rather than reaching for `partitionBy` is a decision
about which guarantee the acceptance criterion rests on. Spark's
`spark.sql.sources.partitionOverwriteMode` defaults to `STATIC`, under which
`mode("overwrite")` on a partitioned write replaces the whole table directory - which
would fail the criterion silently and completely. `DYNAMIC` is the setting that does not,
but then the property the criterion tests is a session config that any caller can change
and that nothing in the repository would notice being wrong. Writing the directory makes
the guarantee structural: the code cannot touch a partition it did not name. It is also
the layout `ingest/raw.py` already writes and both Spark and pyarrow already read, so it
introduces no new shape. The cost is one Spark job per dirty period rather than one for
all of them, which at a monthly grain is a handful.

The grid-membership rule is the part of this that a narrower design would have got
wrong. `docs/adr/0033`'s density is a statement about the whole table: every active
combination carries a row in every period. So a late entry that introduces a combination
never seen before does not dirty one period - it makes rows missing in all of them, and
writing only the closure would leave a grid with holes that no gate would catch, since
`unique` and `not_null` are both satisfied by a row that is simply absent. Removing the
last entry for a combination is the same failure with the sign reversed. Falling back to
a full rewrite is correct, cheap at this grain, and rare: it fires when the ledger gains
or loses an account-and-cost-centre pairing, not when it gains an entry.

**What a run that fails partway leaves, and why that is acceptable here.** A batch
spanning three partitions is not one transaction. Each directory is replaced on its own,
so an interrupted backfill leaves some periods recomputed and some not - a staging layer
that is neither the old one nor the new one. This is the same limitation `docs/adr/0016`
states for the raw layer, and it is more comfortable here for two reasons. Staging is
derived, so nothing in it is lost that raw cannot produce again. And the affected-period
set is not cleared by the layers that consume it - `docs/adr/0039` puts that in the
orchestrator's hands - so an interrupted backfill leaves every period it was working on
still marked as owed, and the next run redoes all of them. The recovery is the same one
the whole design leans on: applying the same computation again reaches the same answer.

Making it atomic would need a manifest naming the files that constitute a version of the
table, which is a second thing that can disagree with the data. `docs/adr/0016` deferred
that to whichever ticket first needs snapshot reads, and this is not it.

A period whose rows have all gone is removed rather than written empty, which matches
`raw.write_partition`. The removal happens instead of a write, never before one, so
there is no window in which a partition has been deleted and its replacement not yet
started.

The dimensions are unaffected. `docs/adr/0026` keeps them rebuilt whole, and at tens of
rows the question does not arise.
