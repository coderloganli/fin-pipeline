# A rate is not an amount: six decimal places, drawn as an integer

summary: Exchange rates are written to six decimal places rather than two, and are drawn as an integer number of millionths so that no float participates in producing one; amounts keep their two places and their own formatter.

## Context

The generator wrote exchange rates through `format_amount`, the same function that
renders a currency amount, and so at two decimal places: `7.87`, `7.81`.

Two decimal places on a rate near 7.87 is a quantisation error of ±0.005, which is
±0.06% on every foreign-currency conversion the platform will ever compute. This
project's entire claim is that the numbers are right and that a rerun reproduces
them. An error of this shape defeats the first half and is *protected* by the second:
every rerun reproduces the same wrong figure, and nothing in the pipeline ever
disagrees with itself. It is the kind of wrong that looks perfectly reasonable and
that nobody thinks to question.

The defect was found from the other end. Writing the source contracts, the first draft
of `fx_rate.yaml` declared `scale: 2` on `rate_to_base` — copied from the amount
columns, because that is what the generator emitted. The stage 8 review pointed out
that this was writing a domain error into the contract. The contract was corrected
there and then: it declares no scale for a rate, and its `min` is `0.000001`. The
generator was not, and the two have disagreed since.

There is a second problem in the same six lines, and it is the one worth more:

```python
centre = Decimal("1.0") if currency == BASE_CURRENCY else Decimal(rng.uniform(5.0, 9.0))
drift = Decimal(rng.uniform(-0.02, 0.02))
```

`Decimal(a_float)` converts the exact binary value of a double — `Decimal(7.87)` is
`Decimal('7.8700000000000001065814103640150278806686401367187500')`. Amounts never do
this. They draw an integer number of cents and divide (`_cents`), which is why
ADR 0006 can say amounts are rendered from `Decimal` and never from float. The rate
path was the exception, and quantising to two places hid it. Quantising to six would
have exposed four more digits of a double's binary expansion.

## Decision

**Rates are written to six decimal places**, through their own formatter. `schema.py`
gains `RATE_PLACES = Decimal("0.000001")` and `format_rate`, beside `AMOUNT_PLACES`
and `format_amount`. Six is not a fresh choice — the contract's `min: 0.000001`
already commits to it, and this brings the producer into line with the statement of
what ingest expects.

**No float participates in producing a rate.** The centre is drawn with `randrange`
as an integer number of millionths, the daily drift is drawn as an integer number of
parts per million, and the arithmetic stays in integers until one exact division by
1,000,000 at the end. `_rate` in `dimensions.py` mirrors `_cents` in `entries.py`.

**The base currency renders as `1.000000`.** CNY is exactly one; a fixed six-place
format writes it in full so the column has one shape and no reader special-cases it.
The contract declares no scale, so either form would validate — consistency decides.

**The range and the drift model are unchanged.** A centre per currency in `[5, 9)`,
an independent daily jitter of ±2%.

## Reasoning

Changing only the quantum — `format_amount` to a six-place variant — would satisfy
the acceptance criterion and fix nothing. A two-place value padded to `7.870000` is
still ±0.06% wrong; the digits have to carry information, which means the draw itself
has to have that resolution. That is why the integer draw is the substance here and
the decimal places are the symptom.

Integers rather than a wider float are the same argument amounts already settled.
A float drawn in `[5, 9)` has far more precision than six places, so it would look
adequate; what it does not have is an exact decimal value, and the moment the pipeline
starts multiplying amounts by rates, where the rounding happened becomes a question
someone has to answer. Drawing the rate as an integer number of millionths makes the
value written to the file exactly the value that was drawn.

**The values change, not just their precision, and now is when that is free.**
`randrange` does not produce the numbers `uniform` produced, so this ticket changes
every rate in the generated data. Nothing consumes a rate today — the point-in-time
join and the base-currency conversion are step two — so the change costs nothing but
a regenerated file. After step two it would cost every number those tests assert on,
and the reproducibility guarantee would make the old figures look authoritative right
up to the point someone checked them against a real rate.

**The drift model is left alone deliberately, and it is wrong.** The daily jitter is
independent rather than a random walk, so two consecutive days can differ by four per
cent, which no real rate series does. That matters to how convincing the
point-in-time join looks — "the March rate, as it stood in March" is a weaker
demonstration against a sawtooth. But it is a modelling question, not a precision one,
and folding it into this change would make "what moved and why" unanswerable across a
diff that already rewrites every rate in the file. It is written down here so that the
step-two task finds it stated rather than discovering it in a chart.
