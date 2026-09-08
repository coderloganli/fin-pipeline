# An adjustment is a fact of its own, not a row in fct_gl_entry

summary: `gl_adjustment` becomes `fct_gl_adjustment`, attributed by the same three
point-in-time joins, because gate 3 requires the rows sharing a `doc_id` to balance and
an adjustment is single-sided by design.

## Context

`gl_adjustment` has been ingested since the incremental load landed and consumed by
nothing: its contract declares `feeds: []`, and `docs/architecture.md` says so. It is
partitioned by `accounting_date` and advances on `posted_at`, exactly like `gl_entry`.

Bringing it into the pipeline is the first half of giving reports an as-reported and an
as-restated view. Where its rows go is the question.

The obvious answer is that they are journal lines and belong in `fct_gl_entry` with a
type column. That answer does not survive gate 3. `vouchers_are_balanced` groups
`fct_gl_entry` by `doc_id` and fails any group whose base-currency debits and credits
differ. The generator writes an adjustment as a standalone row carrying a debit and a
zero credit, with a `doc_id` of its own — it says so in `generator/entries.py`: "An
adjustment is a standalone row rather than a voucher, so the per-voucher vendor rule
cannot apply to it."

So an adjustment folded into `fct_gl_entry` turns a gate red on correct data. The
repairs available are all worse than the disease: exempting adjustments from the gate
makes the gate lie about what it checks, and giving each adjustment a synthetic
balancing line invents a posting the source never made.

## Decision

`transform/spark/facts.py` builds two models rather than one.
`fct_gl_adjustment` carries the adjustment's own columns — including `adjusts_entry_id`
and `adjustment_type` — attributed by the same three range joins on account, cost centre
and rate, converted and rounded at the line by the same code.

`transform/load.py` lands it, `_sources.yml` declares it, a mart model widens it with
names, and `gl_adjustment.yaml` finally declares what it feeds.

It carries the gates that apply to it: primary key uniqueness on `(entry_id, version)`,
referential integrity to the three dimensions, and gate 6's agreement between the
base-currency and original amounts. It does not carry gate 3, because it is not a
voucher and never was.

## Reasoning

The gate is right and the shape it enforces is right. A voucher balances; an adjusting
posting is a delta against a voucher that already balanced. Keeping them in one table
would mean the table's rows no longer answer to one rule, and every consumer would have
to remember a filter to avoid double-counting — the class of mistake that produces a
number somebody publishes.

Two tables cost a join in the aggregate and nothing else. The attribution logic is not
duplicated: it is one function applied to two contracts, which is the same move
`transform/spark/scd2.py` already makes for three dimensions that are not the same
table.

`adjusts_entry_id` is carried and not enforced. It names the line an adjustment revises,
and a referential test against `fct_gl_entry` was considered and left out: an adjustment
can legitimately point at an entry outside the loaded window, and a gate that fires on a
correct backfill is one people learn to ignore. It is a trail to follow, not a
constraint.
