# Every feature comes from the series' own past, and that sets the demo's period range

summary: Lags of 1, 3 and 12 periods, a month indicator and a cost-centre size band
computed from training rows only; lag-12 needs a history the 12-period demo range does
not have, so the generated range moves to 36 periods.

## Context

`ml/README.md` fixed the feature set: lagged balances at one, three and twelve periods,
month indicators, and a cost-centre size band. Two things about it were not settled.

**The range.** `generator/config.py` defaults to `2026-01:2026-12` and `config.json`'s
`try` command generates six periods. A lag of twelve needs a thirteenth period before it
has a single usable row, so on twelve periods the feature is empty for every row.

**The size band.** A band computed over the whole panel is computed partly from periods
a fold is supposed not to have seen, which is leakage through a side channel — the kind
that does not show up in a split assertion because nothing about the split is wrong.

## Decision

**The features, per (account, cost centre) series ordered by period:**

| feature | what it is |
|---|---|
| `lag_1`, `lag_3`, `lag_12` | `balance_as_restated` that many periods earlier, for the same series |
| `month` | the calendar month, one-hot |
| `size_band` | which tercile of median balance the cost centre falls in |

**Lags come off the dense grid directly.** `docs/adr/0033` gives every active
combination a row in every period, zero where nothing posted, so "twelve periods back"
is the row twelve back and needs no gap filling. A sparse table would have made it the
twelfth most recent period with activity, silently.

**Rows without a full set of lags are not trained on and not judged.** The first twelve
periods of a series produce no rows. No imputation: a filled lag is an invented figure
in a repository whose argument is that zero and null are different facts
(`docs/adr/0033`).

**The size band is computed from training rows only**, inside the fold, as terciles of
each cost centre's median `balance_as_restated` over the training periods. A cost centre
not seen in training falls in the middle band.

**The generated range moves to 36 periods.** `config.json`'s `try` generates
`2024-01:2026-12`, and the fixtures the ml tests use generate the same span.

## Reasoning

**36 rather than 13.** Thirteen periods makes lag-12 defined and makes the month
indicator useless: every month appears once, so a one-hot month is a unique label per
row and the model can fit the training data through it alone. Three years gives each
month two observations in training and one to be tested on, which is the least that
makes a seasonal term mean anything. Financial series carrying seasonality is the stated
reason the fixed threshold was rejected in the first place; a range too short to show
seasonality would leave that argument undemonstrated in the data.

**Moving the range rather than dropping lag-12.** Dropping it is the cheaper change and
it removes the only feature that can see a year-ago comparison — in a domain where the
year-ago comparison is the thing analysts actually look at, and which
`agg_monthly_balance` already carries as `balance_pct_yoy` for exactly that reason.

**A size band rather than the cost centre's identity.** One-hot over twelve cost centres
would let the model memorise each one, and the band is what the feature was for: a large
cost centre and a small one have different natural variability, and the model should be
able to say so without being told which is which.

**Terciles of the median rather than of the mean**, because the median of a series
containing a planted anomaly is where the series normally sits and the mean is dragged
by the anomaly — the band would then be partly a function of the thing being detected.

## Consequences

**The demo and the fixtures generate three years.** `config.json`'s `try` changes, and
generation for the ml tests costs more than for the existing suites. The existing
pipeline tests keep their own shorter ranges; nothing forces one range on everything.

**The first twelve periods of the range are never judged.** A 36-period range judges 24.
That is visible in the backtest artefact rather than implied, so nobody reads a fold
count as covering the whole range.

**A planted anomaly inside that window is unreachable, and one of them was.**
`growing_account` planted its rise over the first four periods of any range, so the
effect gate `docs/adr/0054` asks for could not have been written for the concentrated
shape. Moving the range to 36 periods does not fix that on its own — the window moves
with the range. `docs/adr/0056` anchors the planted growth at the middle of the range,
where the long-tail switch already puts its own.

**`ml/README.md` changes**: it says lagged balances at one, three and twelve periods
without saying what that costs, and the range it costs is now written down beside it.
