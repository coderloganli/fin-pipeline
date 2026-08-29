# contracts

One explicit contract per source table: columns, types, nullability, and semantic constraints.

The contract is what makes schema evolution a decision rather than an accident. Adding a column is compatible and passes with a warning. Dropping a column, changing a type, or changing the meaning of a field is incompatible and fails the run, and the dbt lineage graph names the downstream models that would have broken.

## The shape of a contract

```yaml
table: gl_entry
primary_key: [entry_id, version]

columns:
  - name: currency
    type: string
    nullable: false
    allowed: [CNY, EUR, USD, GBP]
  - name: amount_dr
    type: decimal
    nullable: false
    min: 0
    scale: 2

row_constraints:
  - type: not_after
    earlier: accounting_date
    later: posted_at
  - type: exactly_one_nonzero
    columns: [amount_dr, amount_cr]
```

Types are `string`, `integer`, `decimal`, `date`. `date` means `YYYY-MM-DD`. A column
takes only the modifiers its type allows — `scale` on a string is rejected when the
contract is loaded, not when a validator later trips over it.

`row_constraints` carries the rules that span columns rather than qualify one. The
constraint types are a closed set, so a rule cannot arrive as an undeclared key.

**An empty field is null.** Said rather than assumed: otherwise `""` reads as a
perfectly good string and every `nullable: false` means nothing.

## Two things these contracts deliberately do not say

**They do not say what to do when reality disagrees.** Added column compatible,
dropped column incompatible — that is one rule for all five tables and it lives with
the validator.

**They do not constrain what the business legitimately does.** No upper bound on an
amount: an unusually large entry is an anomaly for the model to flag, not a malformed
row. No maximum gap between `posted_at` and `accounting_date`: a late arrival is an
ordinary event. Both look reasonable written against clean data and both would make
the validator cry wolf in normal operation.

The rules here were checked against data generated with every business switch on, not
only against the clean baseline. One candidate rule failed that check and it was the
one that mattered: written from clean data, `dim_cost_center_src` looks like it has
one row per cost centre. Turn on the organisational change and it has two, differing
in `effective_date` — which is the scenario the point-in-time join exists for. The key
is `(cc_code, effective_date)`. See `docs/adr/0008-contracts-are-written-by-hand.md`.
