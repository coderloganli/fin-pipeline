# Source contracts are written by hand, not derived from the generator

summary: Each source table's contract is an independent statement of what ingest expects, and its rules are checked against data with every failure-mode switch on so that legitimate business events are never called violations.

## Context

CSV carries no schema. `291.01` is the text `"291.01"`, `2026-01-29` is the text
`2026-01-29`, and nothing in the file says a column must be present, must be a number,
or may not be empty. Whatever the pipeline is going to check, it has to be written
down somewhere else first — and that is what these contracts are.

The obvious shortcut is to generate them from `generator/schema.py`, which already
declares every column the generator emits. It would be quick, and it would be exactly
wrong.

## Decision

**Contracts are written by hand, one YAML file per source table, under
`ingest/contracts/`.** Nothing under that directory imports `generator`.

A contract states the table's columns and their order, each column's type and
nullability, its permitted values or bounds where those exist, the table's primary
key, and a list of `row_constraints` for the rules that span columns rather than
qualify one — `not_after` for date ordering, `exactly_one_nonzero` for the
debit-or-credit shape. The constraint types are a closed, named set the loader checks,
so a rule cannot arrive as an undeclared ad-hoc key.

An empty field in the CSV is the null value. That has to be said rather than assumed:
without it, a validator could treat `""` as a perfectly good string and the
nullability declarations would mean nothing.

A contract does not state what to do when reality disagrees with it. The rule that an
added column is compatible and a dropped one is not is one rule for all five tables,
and it lives with the validator.

**Every business rule in a contract was checked against data generated with every
business switch turned on** — `late_entries`, `restatements`, `cost_centre_move`,
`unbalanced_vouchers`, `growing_account`, `amount_outliers`, `long_tail_anomaly` —
not only against the clean baseline.

`schema_drift` is deliberately excluded from that set. Its whole purpose is to add or
remove a column, so it is *supposed* to violate the column contract; including it
would make the check contradict itself.

## Reasoning

A contract derived from the producer cannot catch the producer changing. If the
generator gains a column and the contract is regenerated from it, the two agree
forever and the check is theatre. Two independently written statements are what make
disagreement possible, and disagreement is the entire product: a test compares the
hand-written column list against `schema.COLUMNS` and goes red when they drift, at
which point a person decides which one is wrong. That is the contract working, at the
point it is meant to work.

The rule-checking against every switch is the part that is easy to skip and expensive
to get wrong. The generator's switches produce **legitimate business events** — an
entry posted six weeks after its period closed, a restatement, a cost centre moving to
another department mid-year. They are not malformed input. A contract that called them
violations would make the validator cry wolf during ordinary operation, which is the
failure mode that gets validators switched off.

One candidate rule failed that check, and it was the one that mattered. Written from
the clean baseline, `dim_cost_center_src` looks like it has one row per cost centre,
so `cc_code` reads as the primary key. Turn on the organisational-change switch and a
cost centre has two rows, differing in `effective_date` — which is not a duplicate but
the whole point of the dimension. The primary key is `(cc_code, effective_date)`, and
the same applies to `dim_account_src`. Had the clean baseline been the only evidence,
this contract would have rejected the scenario that problems one and two exist to
demonstrate.

Two rules were deliberately not written for the same reason: an upper bound on
amounts, which the outlier and long-tail switches would trip, and a maximum gap
between `posted_at` and `accounting_date`, which the late-arrival switch would trip.
Both look reasonable against clean data and both are wrong.

Cross-table references — `adjusts_entry_id` pointing at `gl_entry.entry_id`,
`account_code` at the account dimension — are absent, because a single-table contract
cannot verify them and this task declines to declare a field it cannot test.
