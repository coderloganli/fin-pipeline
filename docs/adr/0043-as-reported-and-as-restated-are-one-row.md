# as-reported and as-restated are two columns of one row, bridged by a delta

summary: `agg_monthly_balance` gains `balance_as_reported`, `restatement_delta` and
`balance_as_restated`; a correction moves the reported figure, a restatement moves only
the restated one, and master data can later feed the same delta rather than needing a
mechanism of its own.

## Context

`gl_adjustment.adjustment_type` distinguishes a correction from a restatement, and
`docs/product.md` puts the distinction at the centre of what this platform is for: "a
restated figure has to be distinguishable from a late one".

The two are different events. A correction amends the period's number — after it lands
there is one figure and it is the amended one. A restatement keeps the original basis
and presents a new one alongside it, so a report has to be able to answer both "what did
we publish" and "what do we now believe".

`docs/adr/0027` puts the master-data twin of this out of scope, and asks that whatever
is built for entries be a shape master data could later join rather than a second
mechanism.

## Decision

The aggregate keeps one row per `(account, cost centre, period)` — the grain is
unchanged — and carries three balance columns:

- `balance_as_reported` — entries and `correction` adjustments, signed by the account's
  normal side as `docs/adr/0032` already specifies. This is what `balance` was, plus
  corrections.
- `restatement_delta` — the same signing applied to `restatement` adjustments alone.
- `balance_as_restated` — the sum of the two, stored rather than left to the reader.

`debit_total` and `credit_total` stay on the as-reported basis, so
`balance_as_reported` still reconciles from them and the row's arithmetic is checkable
by hand.

The windowed columns — `balance_delta_mom`, `balance_pct_mom`, `balance_delta_yoy`,
`balance_pct_yoy`, `balance_rolling_3m` — are computed over `balance_as_restated`.

The column `balance` is removed rather than kept as a synonym.

## Reasoning

Two columns rather than two rows keeps `docs/adr/0033`'s dense grid and
`docs/adr/0036`'s row-count drift gate arguing exactly what they argue today. A basis
dimension would double every row, and the drift gate's ten percent would then be
measuring a number whose meaning had changed underneath it.

Two columns rather than two tables is `docs/adr/0026`'s rule applied here: a second copy
of an answer is a second thing that can disagree.

The bridging delta is the part that is not merely convenient. `balance_as_restated =
balance_as_reported + restatement_delta` makes the difference between the two views a
figure in its own right — which is the number an analyst asking "what changed" actually
wants — and it is the seam `docs/adr/0027` asked for. If master-data restatement is ever
brought into scope, its effect is another contribution to a delta beside this one, and
`balance_as_restated` stays the sum. Nothing about the as-reported column has to be
renegotiated to allow it.

Deriving the windowed columns from the restated basis follows from what they are for.
The anomaly model and the report should judge the best current answer; a
month-on-month delta computed on a superseded basis would flag movements that are
artefacts of not having looked at the restatement.

Removing `balance` rather than keeping it is deliberate. It currently means "entries
only", which after this change is none of the three things above, and a column whose
name stopped matching its contents is how a wrong number reaches a report.

What that costs is worth naming rather than waving at, because it is more than the mart
model. `balances.py` computes all five windowed columns from `balance`, so the window
expressions move to `balance_as_restated`; `tests/test_balances.py` asserts on it
throughout; `tests/test_mart_load.py` asserts its landed numeric type; and
`tests/test_mart_models.py` names it in comparison fixtures. All of those change with
this record. Nothing outside the repository reads it - `ml/`, `insight/` and `app/` have
not landed - which is what makes now the cheap moment and later the expensive one.

A property worth testing and worth stating: with no restatement in a period,
`restatement_delta` is zero and the two balances are equal. The two views coincide until
something makes them differ, which is what makes the pair meaningful rather than
decorative.

What this does not model, and `docs/adr/0027` already owns: the point in time at which a
figure was published. `balance_as_reported` is "the ledger excluding restatements", not
"what the report said on the day the period closed". Those coincide as long as
restatement is the only thing that revises a closed period, which is true here because
an extract that contradicts a held master-data version fails the run. Making them
independent needs the second time axis that record declines to build.
