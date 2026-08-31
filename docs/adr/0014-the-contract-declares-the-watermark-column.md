# The contract declares the watermark column, and for entries it is `posted_at`

## Context

The incremental load needs to know, for each source table, which column tells it how
far it got last time. The plan and `ingest/README.md` both said "the watermark reads
the source `updated_at`". No source table has an `updated_at` column:
`generator/schema.py` does not emit one and none of the five contracts declares one.

Two ways out. Add the column — to `generator/schema.py` and to the contracts — or
pick a column that already exists and say so.

## Decision

The contract declares it. Two new optional top-level keys:

```yaml
watermark: posted_at          # the column the incremental load advances on
partition_by: accounting_date # the column the raw layer partitions by
```

`gl_entry` and `gl_adjustment` declare both. `dim_account_src`,
`dim_cost_center_src` and `fx_rate` declare neither, and a table that declares no
watermark is loaded in full rather than incrementally.

The loader requires each of them, when present, to name a declared column of type
`date` that is `nullable: false`. A watermark that can be absent cannot be compared,
and the failure would be a row silently skipped rather than an error.

## Reasoning

`posted_at` already means what a watermark needs to mean. Its contract comment says
"When it actually landed. May be long after accounting_date closed" — it is the
arrival timestamp, which is exactly what an arrival watermark reads.
`accounting_date` would be the wrong one: it is where the entry belongs in accounting
terms, and a late entry has an old one by definition.

Adding `updated_at` would have put the definition of a column ingest depends on into
the generator, and `docs/adr/0008-contracts-are-written-by-hand.md` exists to keep
those apart. A contract that reads a column the producer invented for it cannot catch
the producer changing that column.

The cost is real and is not hidden: `posted_at` is a date, so the watermark advances
a day at a time and an update that arrives later on the same day is not distinguished
by it. The overlap window is what covers that, and it is why the window exists rather
than being a precaution. A second-resolution watermark would have needed a narrower
window; a day-resolution one needs a wider one. See
`docs/adr/0016-a-merge-rewrites-the-affected-partitions-whole.md`.

Declaring the two keys independently rather than inferring one from the other is
deliberate. "Has a watermark, therefore is partitioned" would be a rule nobody wrote
down, and the first table that wants one without the other would have to break it.
