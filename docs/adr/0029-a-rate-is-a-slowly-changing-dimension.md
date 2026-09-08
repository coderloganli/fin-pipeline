# An exchange rate is a slowly changing dimension, and the fact build has one join shape

## Context

Building `fct_gl_entry` means attributing each entry to three things as they stood on
its accounting date: the account's place in the chart, the cost centre's department,
and the exchange rate for its currency.

The first two come from `dim_account` and `dim_cost_center`, which
`docs/adr/0023` through `0026` already model as validity intervals — a version runs
from the date it took effect until the day before the next one, and the join is
`accounting_date BETWEEN valid_from AND valid_to`.

The rate looked like a different problem. `fx_rate` is keyed on
`(currency, rate_date)` with one row per published day, so the obvious join is an
equality on the date. The design note describing this step called it a range join
without saying what the range was.

The obvious join stops working the moment the source stops publishing every day, which
is what a real rate feed does — no weekend, no public holiday. Measured against the
generated ledger, **27% of entries carry an accounting date that falls on a Saturday or
a Sunday.** An equality join would leave a quarter of the ledger with no rate.

## Decision

`fx_rate` becomes a third model in `transform/spark/scd2.py`: natural key `currency`,
effective column `rate_date`, attribute `rate_to_base`. It produces `dim_fx_rate` with
`valid_from`, `valid_to`, `is_current` and a surrogate key, built by exactly the code
that builds the other two.

The fact build therefore has one join shape, used three times:

    <natural key> = <natural key>  AND  accounting_date BETWEEN valid_from AND valid_to

Both halves, always. The equality is on a different column each time -
`account_code`, `cost_center_code` against `cc_code`, `currency` - and the range half is
identical. Written with the range alone it is a cross join: every fact would match every
version valid on its date, across every account.

`fx_rate.yaml` declares `rows_are_immutable: true`. A given day's rate does not change.

## Reasoning

"The rate in force on the transaction date" and "the department in force on the
transaction date" are the same sentence. Writing two mechanisms for it would mean two
places to get the boundary wrong, and the boundary — does a version start on its
effective date or the day after — is where this class of bug lives. One construction,
asserted once, covers all three.

The equality half is what differs between the three, and it is the half a reader
supplies from context. That is why it is written out above: the failure it prevents is
silent multiplication rather than an error, and a fact table with four times the rows it
should have still sums to a number somebody might publish. Asserting that
`(entry_id, version)` stays unique in `fct_gl_entry` is the cheap guard, and it is in the
test cases for that reason.

The reuse is not superficial. Three properties the dimensions already needed turn out
to be exactly what the rate needs:

The **interval construction** answers the weekend without a special case. Friday's rate
runs until the day before the next published rate, which is Monday, so Saturday and
Sunday fall inside Friday's interval by construction rather than by a lookup rule
written for them.

The **sentinel end** — `9999-12-31`, `docs/adr/0024` — means an entry dated after the
last published rate still joins, instead of dropping out of an inner join with nothing
raised. That was argued for the current dimension version and applies unchanged.

The **attribute-hash collapse** means two consecutive days at the same rate become one
interval rather than two rows saying the same thing. That had no scenario on the
dimensions; here it will happen whenever a rate is unchanged, which is ordinary.

One thing the reuse does not give for free: `scd2.py` types only the interval columns
and leaves attribute columns as the text the raw layer holds. Both existing dimensions
declare their attributes as strings, so nothing noticed. `rate_to_base` is declared
`decimal`, so making the rate a model is what exposes it — and `docs/adr/0026` says
staging is typed. The loader is changed to cast every column to the type its contract
declares, which is a no-op for the two dimensions and makes 0026 true rather than
half-true.

What this gives up is the ability to say "there was no rate published that day" as
distinct from "the rate had not changed". After the collapse both are one interval, and
the fact carries the rate that applied without recording that it was carried forward.
That is the right trade for a ledger — the figure is what matters, and the publication
calendar is the rate feed's business — but it means this layer cannot answer how stale
a rate was. If that is ever wanted, it is a column on `dim_fx_rate` recording the
published date the interval started from, not a change to the join.

The alternative considered was a window function over a broadcast rate table: for each
fact, the maximum `rate_date` at or below `accounting_date`. It gives the same answer.
It was rejected because it is a second way of expressing an idea the repository already
expresses, and because it puts the correctness of the rate lookup in the fact job
rather than in a table anyone can read and assert on. `dim_fx_rate` can be tested for
non-overlap and gaplessness on its own, before any fact touches it.
