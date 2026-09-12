# The planted growth sits where the anomaly model can see it

summary: `growing_account` planted its rise into the first four periods of the range;
the anomaly model cannot judge a period before its thirteenth, so the window is anchored
at the middle of the range the way the long-tail switch already is, and the switch now
requires twice `GROWTH_MONTHS`.

## Context

`docs/adr/0055` excludes the first twelve periods of a range from judging: a row has no
`lag_12` until then, and the design refuses to impute one. `docs/adr/0054` gates the
anomaly layer on catching what the generator planted.

Those two do not currently meet. `generator/entries.py` plants the concentrated rise
over `months[:GROWTH_MONTHS]` — the first four periods of whatever range is generated.
Every planted growth therefore lands inside the window the model never judges, and the
effect gate for the concentrated shape cannot be written at all.

The long-tail switch does not have this problem. It already anchors its raised period at
`months[len(months) // 2]`, so on a thirty-six period range it lands at period eighteen,
which is judged.

## Decision

**The growth window starts at `months[len(months) // 2]`** and runs `GROWTH_MONTHS`
periods from there, matching what the long-tail switch does.

**The switch requires `2 * GROWTH_MONTHS` periods** rather than `GROWTH_MONTHS`, because
the window now has to fit after the anchor. The refusal keeps its shape: a switch that is
on and quietly does nothing is the failure the existing guard exists to prevent.

**Nothing else about the shape changes.** Same reserved accounts, same single supplier,
same `GROWTH_FACTOR`, same `X-GROW-` identifiers, same dedicated random stream. The rise
is still constructed rather than discovered, and it still moves no other switch's data.

## Reasoning

**The generator's job is to plant failures the layers downstream can be tested against,
and one of those layers now has a warm-up.** A planted anomaly that no consumer can
reach is not a test fixture, it is an unused branch. The alternative — weaken
`docs/adr/0055` so early periods are judged with an imputed `lag_12` — trades a real
property of the model for the convenience of the fixture, which is the wrong way round.

**Anchoring at the middle rather than adding a configuration knob.** A `growth_start`
setting would let the ml fixture place the window where it needs it and leave the
default where it is, which is the smaller diff. It was declined: a switch whose planted
period depends on a second setting has two ways to be wrong, and the repository already
has one convention for where a planted anomaly goes — the long-tail switch's. Two
switches placing their anomalies by the same rule is one rule to know.

**The existing generator tests do not depend on the position.**
`tests/test_generator.has_growing_account` scans for three consecutive month-on-month
rises of at least `GROWTH_RATIO` anywhere in the series, and
`test_growing_account_refuses_a_range_too_short_to_show_growth` uses a two-period range,
which is below the new floor as well as the old one. That is what makes this change
cheap, and it is why it is being made here rather than deferred.

## Consequences

**The floor rises from four periods to eight.** A caller generating between four and
seven periods with `growing_account=True` used to get an anomaly and now gets a refusal
naming the new minimum. Nothing in the suite or in `config.json` generates such a range
with that switch on.

**The concentrated and long-tail shapes now land in the same period.** Both anchor at
`len(months) // 2`, so a ledger generated with both switches on raises two different
accounts in one month. They are on reserved accounts that no ordinary voucher touches
(`docs/adr/0007`, `docs/adr/0021`), so they do not interfere; and a period carrying both
shapes at once is a better fixture for the insight layer's comparison than two periods
carrying one each.
