# A merge rewrites the affected partitions whole, and the watermark moves last

## Context

The incremental load reads the source rows at or above
`stored watermark - overlap window`, and has to land them into a raw layer that
already holds earlier runs' rows. Parquet files cannot be updated in place, so
"MERGE" here has to be built rather than issued.

The load also has to survive being interrupted. A nightly run that dies halfway is
the ordinary case this whole ticket exists for.

## Decision

The incoming batch is grouped by accounting period. For each period that the batch
touches, and only those, the existing file is read, the incoming rows are applied by
primary key with the incoming row winning, the result is sorted and written to a
temporary file in the same directory, and that file is moved over the old one.
Periods the batch does not touch are not read and not rewritten.

Within one batch, a repeated primary key collapses to the last occurrence in file
order. The validator is what reports that the source repeated a key; the merge must
not turn it into two rows.

A row's partition comes from its accounting date, which is not part of its primary
key, so the merge alone cannot keep that key unique. After the touched periods are
written, every partition of the table is asked which of the batch's keys it holds -
reading the key columns only - and any key that now belongs to a different period is
removed from the partition still holding it. A partition holding none of them is not
rewritten; a partition left with no rows is deleted rather than left empty.

The watermark is written to `data/raw/_state/watermarks.json` after every partition
has been written, never before, and only ever forward: the stored value is the
maximum of what was stored and the highest watermark value in the batch.

The overlap window defaults to 7 days and is set with `--overlap-days`.

## Reasoning

Rewriting the affected partitions is what makes the load idempotent without needing a
transaction: applying the same rows to the same partition twice reaches the same
file, because the result is a function of the union, not of the sequence of writes.
Appending and de-duplicating on read would have moved the cost to every reader and
made the acceptance criterion — the raw layer's row count is stable across reruns —
false at the layer where it is checked.

Not rewriting the untouched periods is the point of partitioning by accounting period
at all. A late correction to March rewrites March, and the other eleven months are
rewritten only if one of them is still holding a row that has moved out of it.

The key sweep is the exception to "the other months are not opened", and it is a
narrow one: Parquet is columnar, so asking a partition for its primary keys reads the
key columns and none of the rest, and nothing is rewritten unless a key is actually
found in the wrong place. It was added because the alternative was worse than the
cost. Without it, a correction that re-dates an entry into another period without
bumping its version leaves the old partition's copy in place, and the raw layer ends
up carrying two rows for one primary key - which is the single property this whole
decision record exists to establish. A merge that cannot keep its own key unique is
not a merge.

The memory cost is stated rather than hidden, as in
`docs/adr/0011-validation-streams-and-caps-its-findings.md`: peak memory is one
partition plus the incoming batch, plus one partition's primary keys during the
sweep. The batch is bounded by the watermark window, and
the partition by one accounting period — neither is bounded by the size of the table,
which is the property that matters.

Writing to a temporary file and moving it is what keeps a partition from being left
half-written. A crash then leaves some periods updated and some not, with the
watermark still where it was; the next run re-reads the same window and converges,
because applying the same rows again is what a merge does. This is the sequence the
ordering exists for: a watermark advanced before the write would turn an interrupted
run into permanently missing rows, which is the failure mode that does not announce
itself.

Seven days is a starting value, not a measured one. It has to exceed the largest gap
between a row becoming visible in the source and the load running; with a
day-resolution watermark (see
`docs/adr/0014-the-contract-declares-the-watermark-column.md`) it also has to absorb
same-day updates that arrive after the run. A week of daily runs is a wide enough
margin for both, and the flag exists because the right number is a property of the
source system rather than of this code.

One thing this does not give: a run that fails partway has still changed the raw
layer. Each partition is replaced atomically, but a batch spanning three of them is
not one transaction, and a full reload that writes January and then fails to remove a
stale February leaves a table that is neither the old one nor the new one. The
recovery is the same one the whole design leans on - the watermark did not move, so
the next run reads the same window and converges, and the key sweep removes whatever
was left in the wrong place. Making the multi-partition write atomic would need a
manifest naming the files that constitute a version of the table, and a manifest is a
second thing that can disagree with the data. That belongs to whichever ticket first
needs snapshot reads, not to this one.

An update that arrives after the window has passed is missed, and that is a genuine
limitation of a watermarked load rather than a defect to fix here. The backfill path
— `--full`, which ignores the stored watermark and reads the whole source — is what
recovers from it, and it reaches the same raw layer because the merge is the same
merge.
