# Base-currency conversion rounds at the line, not at the total

## Context

Each entry line carries an amount in its own currency and joins a rate. The
base-currency figure is the product, and a product of a two-place amount and a
six-place rate has eight decimal places. Somewhere it has to be rounded to the two
places a currency amount has.

There are two places to do it: on each line, so `fct_gl_entry` stores a rounded amount;
or nowhere, keeping full precision through to the aggregate and rounding the total.

They do not agree. The sum of rounded lines is not the rounded sum, and the difference
grows with the number of lines - a few units in the last place per thousand entries.

## Decision

Every line's base-currency amount is rounded to two decimal places in
`fct_gl_entry`, and `agg_monthly_balance` sums those already-rounded amounts.

Amounts and rates are cast from the raw layer's text to `DecimalType` and never to a
floating-point type. Both precisions are named constants rather than inferred:
`decimal(18, 2)` for an amount and `decimal(18, 6)` for a rate, which Spark multiplies to
`decimal(37, 8)` — measured, not assumed — and `round(_, 2)` reduces to `decimal(32, 2)`.
The tests assert those precisions exactly, not that they are large enough.

## Reasoning

The acceptance criterion settles it: the aggregate has to agree with the entries added
up. An analyst who filters `fct_gl_entry` to one account and one month and totals the
column has to get the number the report shows. Keeping unrounded precision inside the
aggregate would break that by construction, and the discrepancy would appear exactly
where trust in the platform is checked - somebody reconciling a report against its
detail.

It is also what a ledger does. A posted line is an amount, not an intermediate: it is
the figure that would appear on a document, be paid, be reconciled against a bank
statement. A stored value with eight decimal places would be claiming a precision the
business does not have.

The cost is real and is stated rather than hidden: the platform's monthly totals differ
from the mathematically exact conversion of the monthly totals, by an amount bounded by
half a cent per line. That is the ordinary arithmetic of a multi-currency ledger, it is
the same answer any accounting system gives, and the alternative - a total that no set
of lines adds up to - is worse in the one place it would be noticed.

Decimal rather than float is not a separate decision so much as the continuation of
one. `docs/adr/0013` spent a whole ticket keeping floats out of rate generation,
because a float has no exact decimal value and the moment amounts are multiplied by
rates, where the rounding happened becomes a question somebody has to answer. Reading
those rates back and multiplying them as doubles would have made that ticket
pointless. The raw layer holds text precisely so the reader chooses the type; this
reader chooses `DecimalType`.

Pinning the precision rather than trusting it matters because of how the arithmetic
fails. 37 is one short of Spark's maximum of 38, so a slightly wider amount type — a
`decimal(20, 2)` chosen by somebody widening a column — pushes the product past the cap,
and at the cap Spark **reduces the scale rather than raising**. That is a quiet loss of
precision in exactly the calculation this record exists to protect. An assertion that the
precision is exactly 37 fails the moment it happens; an assertion that it is large enough
would not.

Rounding is half-up, which is what `round` does in Spark and what a
ledger means by rounding. `bround` is half-even and would give `0.12` where this gives
`0.13`; a test constructs that tie so the two cannot be swapped by accident. Banker's rounding is defensible and is not used, because
nothing here accumulates enough rounding for the bias argument to bite and half-up is
what a person checking the arithmetic by hand will do.
