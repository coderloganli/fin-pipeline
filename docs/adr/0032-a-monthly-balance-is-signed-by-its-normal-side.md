# A monthly balance is signed by the account's normal side, as it stood in the period

## Context

`agg_monthly_balance` gives one figure per account, cost centre and period. The anomaly
model in step three judges whether that figure falls outside its prediction interval,
and the application shows it to an analyst.

A ledger line is either a debit or a credit, never both. Summing them gives two totals,
and a single figure has to combine them. `debits - credits` is the obvious choice and
it is signed by convention rather than by meaning: an expense account grows on the
debit side, so it comes out positive, while a revenue account grows on the credit side
and comes out negative. Revenue growing then reads as the number going further below
zero.

## Decision

The balance is signed by the account's normal side. Assets and expenses are
debit-normal, so their balance is `debits - credits`; liabilities, equity and revenue
are credit-normal, so theirs is `credits - debits`. Every account's ordinary activity
therefore reads as a positive number that grows.

The account type used is the one attributed to the entry by the point-in-time join -
the type in force during the period being reported, not today's.

`debit_total` and `credit_total` are kept as their own columns.

## Reasoning

Neither the report nor the model should have to know which way an account points. An
analyst comparing this month's marketing spend to last month's is asking whether it
went up; a model fitting a prediction interval to a series is looking for departures
from a level. Both work on "the number got bigger", and a convention where half the
chart means the opposite makes every consumer restate the same rule - which means every
consumer is a place to get it wrong. Encoding it once, in the layer that already knows
the account type, is the only place it is cheap.

Taking the type from the point-in-time attribution rather than from the current
dimension is what keeps this consistent with everything else here. An account that is
reclassified mid-year does not retroactively flip the sign of periods that closed
before the change; March is reported the way March was. This falls out of the join
rather than being arranged, which is the point of doing the attribution first.

Keeping both totals costs two columns and preserves what the sign convention throws
away. A month with large offsetting debits and credits and a small net is a different
month from one with almost no activity, and the net alone cannot tell them apart - a
distinction that matters to an anomaly model, which would otherwise see a quiet month
and a busy balanced month as the same observation.

What this gives up: a contra account - accumulated depreciation, an asset account that
grows on the credit side - is signed as an asset and so reports its growth as
increasingly negative. The chart in `docs/adr/0021` has one, `1602`. Handling it would
mean a per-account normal side rather than a per-type one, which is a column on the
source dimension and therefore a claim about what the ERP exports. It is not worth
inventing that for a single account nothing yet reports on. If a contra account ever
carries a figure anyone reads, the fix is a column in the chart, not a special case
here.
