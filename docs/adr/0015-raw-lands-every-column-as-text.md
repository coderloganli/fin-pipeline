# Raw lands every column as text, one Parquet file per accounting period

## Context

The raw layer's job, per the layering table, is to land the source as it arrived:
no cleaning, no retyping, no de-duplication beyond the primary key the merge is
built on. It is written as Parquet, partitioned by accounting period.

Parquet is typed, so the question the CSV never had to answer arrives here: does
ingest convert `amount_dr` to a decimal and `posted_at` to a date on the way in?

## Decision

Every column is written as Parquet `string`, in the order the contract declares, and
nothing else is written beside them. An empty CSV field lands as an empty string, not
as a Parquet null.

Each partition is one file:
`data/raw/<table>/accounting_period=YYYY-MM/part-0000.parquet`. A table with no
`partition_by` in its contract is one file at `data/raw/<table>/part-0000.parquet`.

Rows inside a file are sorted by the contract's primary key, comparing the key
columns as the text they are.

## Reasoning

Converting types here would move a decision into the layer that is defined by not
making it. `staging` unifies types; that is its line in the table. A value that does
not parse is a validation finding, and `ingest/validate.py` is where it is raised — a
loader that also parsed would either raise a second time or, worse, coerce quietly.

Text also makes raw a faithful record of what arrived, which is what makes it useful
when a downstream number is wrong: the question is always whether the source said
that, and a raw layer that already reinterpreted cannot answer it.

Empty string rather than null for the same reason. `contracts.is_null` says an empty
field is the null value, but that is a statement about what a contract *means*, and
applying it is a transformation. Raw records the field; staging applies the meaning.

One file per partition rather than one per run: appending a file per run makes a
rerun visible in the layout even when the rows are identical, and the row count that
the acceptance criterion reads would then depend on how many times the load ran. It
also produces exactly the small-file problem the transform layer would otherwise have
to compact away.

Sorting by primary key makes a partition's contents a function of its rows and not of
the order they arrived in, which is what lets two runs be compared at all. The
comparison is on text: interpreting `version` as a number would sort `10` before `2`,
which is a defensible reading and still a type decision this layer does not make.
