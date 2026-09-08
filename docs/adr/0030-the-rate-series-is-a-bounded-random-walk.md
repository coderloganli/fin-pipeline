# The rate series is a bounded random walk, and it is not published at the weekend

## Context

`docs/adr/0013` fixed the precision of an exchange rate and closed with a defect it
deliberately left open:

> The daily jitter is independent rather than a random walk, so two consecutive days
> can differ by four per cent, which no real rate series does. That matters to how
> convincing the point-in-time join looks — "the March rate, as it stood in March" is a
> weaker demonstration against a sawtooth. But it is a modelling question, not a
> precision one [...] It is written down here so that the step-two task finds it stated
> rather than discovering it in a chart.

This is that task. Measured on the generated data, EUR moves +2.34% from 1 January to
2 January and -0.98% the next day. Real EUR/CNY moves on the order of 0.3% in a day.

`fx_rates` also publishes a rate for all 365 days. A real feed publishes on business
days only.

## Decision

**The series is a random walk.** Each day's rate is the previous day's rate multiplied
by a small perturbation, rather than an independent draw around a fixed centre. The
daily band narrows from ±2% to ±0.3%.

**The walk is pulled back towards its centre.** Each step also moves a fixed fraction
of the distance from the current value back to the currency's centre.

**No float, still.** The perturbation and the pull are drawn as integer parts per
million, the arithmetic stays in integers, and there is one exact division at the end -
the discipline `docs/adr/0013` established, unchanged.

**Saturdays and Sundays are not published.** Every currency still draws on every
calendar day, including the base currency whose draw is discarded, so that the
stream stays aligned; the weekend's drawn value is simply not written.

**The centre, the currency range and the base currency are unchanged.** CNY is exactly
`1.000000`.

## Reasoning

The sawtooth undermines the thing this step exists to demonstrate. "The rate that was
in force in March" is an argument about time semantics, and it is weakest when the
series has no memory: against noise around a constant, any date's rate is as good as
any other's, and getting the join wrong costs nothing visible. A walk makes the March
rate genuinely different from today's, so a wrong join produces a wrong figure rather
than a differently-wrong-looking one.

That is a claim about signal against noise, so it was measured rather than asserted.
Over three currencies and three seeds: the mean relative difference between any two
months' averages, against the mean day-to-day move.

| | month-to-month | day-to-day | ratio |
|---|---|---|---|
| independent jitter | 0.248% | 1.305% | 0.19 : 1 |
| random walk | 0.734% | 0.150% | 4.88 : 1 |

Noise drowning the signal five to one, become signal over noise five to one. Both halves
moved: the walk accumulates, so months genuinely diverge, and the narrower band stops a
single day from swamping the difference. A test pins the ratio, because every other
assertion here — the band, the weekend, reproducibility — is one a retuned sawtooth would
still satisfy.

The pull matters more than it looks. A pure random walk is unbounded: at ±0.3% a day
the standard deviation after a year is about 5.7%, which is harmless over the twelve
months generated today, but the date range is a parameter and step four will widen it.
A rate that wandered out of a plausible band would be a different kind of wrong from
the one being fixed here, and a clamp at the boundary would bias the series against it.
The pull keeps the walk inside its band by construction rather than by luck, and it is
also what real rates do. Driven over ten years, the non-base currencies stay within
[5.47, 6.65].

The band it keeps them in is the reachable one, not the draw range. A centre is drawn
from [5.000000, 9.000000) and the first step moves off it before anything is written, so
a rate just outside the draw range is ordinary rather than a defect - the boundary draws
produce 4.985300 and 9.026458. What the pull guarantees is that the series does not
wander away, not that it stays inside the interval its centre came from. The tests
assert the reachable band, and the two are stated separately here because conflating
them is how a correct value comes to look like a bug.

Its strength trades the two properties against each other, and 2% of the gap per day was
chosen by measuring rather than by taste. Weakening it widens the month-to-month drift —
at 0.2% the drift roughly doubles — but also lets a currency whose centre sits near an
edge of the band approach it. At 2% the signal already exceeds the noise about five to
one, which is the property that was missing; buying more drift at the cost of the bound
would be paying for something already sufficient.

Not publishing at the weekend is what makes the as-of join load-bearing rather than
decorative. 27% of generated entries carry an accounting date that falls on a Saturday
or Sunday, so under an equality join a quarter of the ledger would silently lose its
base-currency amount. The scenario is abundant rather than contrived, and it is
generated by the ordinary path rather than by a switch, because a rate feed that skips
the weekend is not a failure mode - it is what a rate feed is.

**Doing it now rather than later is the whole reason it is here.** ADR 0013 already
made the argument: nothing consumes a rate today, so changing every rate in the
generated data costs a regenerated file. The moment this ticket lands, `fct_gl_entry`
and `agg_monthly_balance` assert on figures derived from these rates, and changing them
would rewrite every expected number in the suite. Worse, the reproducibility guarantee
would make the old figures look authoritative right up to the point somebody compared
one against a real rate. This is the last free window and it closes with this ticket.

The public-holiday calendar is not modelled. Weekends give the gap the join has to
handle, and a holiday is the same gap with a longer tail; a calendar per jurisdiction
would be a table nobody reads to demonstrate a behaviour already demonstrated.
