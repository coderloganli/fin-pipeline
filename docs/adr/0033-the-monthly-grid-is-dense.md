# The monthly grid is dense, and null is not zero

## Context

`agg_monthly_balance` carries period-over-period and year-over-year comparisons and a
rolling three-month mean. All three are window functions over the rows of one account
and cost centre, ordered by period.

A ledger does not post to every account every month. If the table holds only the months
that had activity, the row before March in that window might be January.

## Decision

Every combination of account and cost centre that has activity in any period carries a
row for **every** period in the generated range. A period with no entries has zero
totals and a zero balance.

The range of periods is an argument — `--periods 2026-01:2026-12`, the same shape the
generator takes — and not derived from the facts. Without it the job falls back to the
span of `accounting_date` in `fct_gl_entry` and says in its output which it used.

Each comparison is two columns, not one:

| column | definition | null when |
|---|---|---|
| `balance_delta_mom` | this period minus the previous one | there is no previous period |
| `balance_pct_mom` | that delta over the absolute value of the previous balance | no previous period, **or** the previous balance is zero |
| `balance_delta_yoy` | this period minus the same period a year earlier | there is no such period |
| `balance_pct_yoy` | as above | no such period, or that balance is zero |

`balance_rolling_3m` is the mean over the periods available, and `rolling_periods`
records how many that was.

A comparison with no counterpart is `null`. It is never zero, and it is never NaN.

The rolling mean over fewer than three periods is computed over the periods that exist,
and a companion column records how many were used.

## Reasoning

The gap is the defect. "Compared with last month" computed over a sparse table silently
becomes "compared with the last month that had activity", and the two agree exactly
when nothing interesting happened. An account that goes quiet for a quarter and then
resumes would report its comparison against the month before the silence, labelled as
if it were the month before - a wrong number that looks right, which is the failure
mode this project keeps choosing to design against.

Density also makes the series a series. Step three fits a model to lagged balances, and
a lag over an irregular index is not the lag the model assumes; the zero months are
real observations - the account really did have no activity - and dropping them would
teach the model that the account is busier than it is.

Deriving the range from the facts is the boundary bug this record is about, one level
up. `fct_gl_entry` carries the dates entries were posted on and nothing about the range
the report covers. Taking `min` and `max` of them makes the grid dense between the first
and last entry, which passes every test about an interior gap — and silently drops a
leading or trailing period in which nothing was posted anywhere. That is the same class
of error as the sparse table, and it hides in the one place the sparse table's tests do
not look. "Which periods does this report cover" is a question the data cannot answer,
so the caller answers it; the fallback exists so the job is usable by hand, and it
announces itself so nobody mistakes it for the range they asked for.

Two columns per comparison rather than one, because either alone is unusable somewhere.
A percentage is what a reader wants — "marketing is up 40%" — and it is undefined when
the previous period was zero, which is exactly the case an anomaly investigation cares
about most: an account that had nothing and now has something. A delta is always
defined and says nothing about scale. Carrying both means the undefined case is a null
in one column beside a real number in the other, rather than a choice between a null
that loses the fact and a zero that invents one.

The denominator is the absolute value of the previous balance. Without that, a balance
moving from -100 to -50 reports as -50% — a fall, when the account moved towards zero —
because the sign of the denominator flips the sign of the ratio. Sign conventions are
already settled in `docs/adr/0032`; the comparison must not reintroduce a second one.

Zero and null are different facts and the distinction has to survive into the table.
Zero says the ledger was posted to and the total came to nothing, or that no entry
landed - either way, a real measurement of the period. Null says there is nothing to
compare against, because the period is the first one. A model that treats a leading
null as zero learns a jump that never happened; an analyst who sees 0% growth in the
first period reads it as stability rather than as an absent baseline. NaN is not used at
all: it propagates through arithmetic silently and compares false with itself, so a
filter written to exclude it and a filter written to find it can both come back empty.

Stating the rolling window's width rather than only its value is the same argument one
level down. A three-month mean computed from one month is a number, and without the
count beside it there is no way to tell it from a three-month mean that had three
months. The alternative - leaving the mean null until three periods exist - throws away
information that is useful at the start of the range, which is exactly where the anomaly
model has the least to work with.

The cost is rows: the grid is every active combination times every period, where the
sparse table would be only what was posted. On this chart and these cost centres that is
a few thousand rows. If a real chart ever made that a problem, the answer would be to
densify at read time in the model that needs it rather than to store gaps and hope every
consumer remembers - the correctness argument does not change, only where the expansion
happens.
