# The long-tail anomaly raises amounts rather than adding rows

summary: A dedicated account carries the same number of entries whether the switch is on or off, and the switch multiplies their amounts; its amounts sit in a narrow band because the top-twenty share depends on it.

## Context

Two shapes of anomaly matter to this project, and they are opposites. A concentrated
one puts most of an increase into a handful of large entries, so looking at the
largest few finds it. A long-tail one spreads the same increase across hundreds of
small entries, so the largest few look entirely ordinary and finding it means asking
a different question.

The whole point of building the second shape is the comparison in step three: an
insight layer handed a fixed slice of top-N entries and one that can query for itself
score the same on concentrated anomalies. Long-tail is the only shape that separates
them. Without it the comparison table is flat and the more elaborate implementation
has nothing to show for itself.

The three existing switches that touch entries all work the same way — they append
rows carrying an `X-` prefix, which leaves the ordinary rows untouched and keeps each
switch independent of the others.

## Decision

**The long-tail switch does not append rows. It multiplies amounts.**

Two accounts are reserved, `6996` for the debit side and `6995` for the credit side.
They receive three hundred vouchers per period **whether or not the switch is on**.
The switch multiplies the amounts of one period's three hundred by 1.6.

**Their amounts are drawn from 250–350**, narrower than the 100–500 the ordinary
entries use.

Both reserved accounts are emitted into `dim_account_src` unconditionally, as the
growth anomaly's accounts are.

## Reasoning

Copying the append-a-row pattern would have been less work and would have been wrong.
A long-tail anomaly that adds three hundred rows changes the entry count, and **a
steady entry count is the diagnostic feature of this shape** — it is how you tell
"everyone spent a bit more" from "we did more business this month". The product
document's worked example turns on exactly that observation: 312 entries this period
against 297 last, flat, so the rise is in the amounts. A generator that moved the
count would be producing a scenario the documentation says it is not, and the insight
layer could then find the anomaly by counting rows, which is not the skill being
tested.

Keeping the rows present when the switch is off is what makes multiplication possible
without disturbing anything else. The alternative — generating them only when the
switch is on — would make the count jump from zero to three hundred, which is the
same defect wearing a different hat.

The narrow amount band is the part that looks arbitrary and is not. The acceptance
threshold is that the largest twenty entries account for less than ten per cent of the
increase. Under a uniform uplift the increase is proportional to the amount, so that
share is exactly the top twenty's share of the account's total — the uplift factor
cancels out, and only the entry count and the spread of amounts matter.

That makes the worst case computable rather than something to sample: the largest
twenty all at the top of the band, the remaining two hundred and eighty all at the
bottom.

    band 250-350:   20 x 350 / (20 x 350 + 280 x 250) =  9.09%
    band 100-500:   20 x 500 / (20 x 500 + 280 x 100) = 26.32%

The narrow band satisfies the threshold for every possible draw; the ordinary band
does not satisfy it at all. This bound replaced an earlier argument that ran three
hundred seeds and reported the largest share observed, 7.83%. Sampling shows only
that no counterexample turned up. The bound shows that none exists. Either way, left
undiscovered until the tests were written, this would have arrived as a mystifying
red test rather than as arithmetic.

One thing here is a margin rather than a guarantee, and it is worth saying so. The
predicate compares the raised period against the mean of the others, and each period's
total is drawn independently, so a 1.6 uplift does not *prove* the ratio clears the
1.5 threshold. Measured over two thousand seeds: a single period's total has a
relative standard deviation of 0.54%, the ratio averages 1.5998 with a standard
deviation of 0.0090, and the worst case seen is 1.5663. The threshold sits 11.1
standard deviations below the mean. That is a margin, not a proof, and it is recorded
here so a later change that widens the band or cuts the voucher count knows what it is
spending.

Nothing was tuned to make a test pass. An account whose amounts vary by a factor of
five does not have a long tail to begin with; the accounts that do are the ones full
of similar small claims — travel, office supplies — which is what a 250–350 band
describes. The threshold and the shape agree because they are describing the same
thing.
