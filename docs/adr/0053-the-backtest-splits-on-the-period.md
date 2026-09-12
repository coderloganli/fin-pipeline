# The backtest splits on the accounting period, not on row position

summary: The training data is a panel of ~250 series over the same months, so folds are
cut on the period axis by a splitter written here; `TimeSeriesSplit` splits contiguous
row positions and would put the same month on both sides of a fold.

## Context

The acceptance asks for a time-series split rather than a random one. The reason is
leakage: a random split trains on next year's figures and tests on last year's, and the
score that comes back describes nothing.

What scikit-learn offers is `TimeSeriesSplit`. Its documentation is explicit that it
splits by **contiguous row index position** — the first k folds train, the (k+1)th
tests — and that successive training sets are supersets of the previous ones. It has no
notion of a time column. scikit-learn provides no group-aware time-series splitter;
verified against the cross-validation documentation, which lists `TimeSeriesSplit` as
the only time-series splitter and offers sorting or a custom iterator as the workaround.

This data is not one series. `agg_monthly_balance` is a dense panel: every active
(account, cost centre) pair carries a row for every period in range, per
`docs/adr/0033`. Roughly 250 series over 36 periods. Handing that to `TimeSeriesSplit`
sorted the natural way — by series, then period — cuts the folds between *series*, and
every fold trains on one set of cost centres to predict another over the same months.
Sorting by period first makes the cut fall in a sensible place only as long as every
period has exactly the same number of rows, which the dense grid happens to give and
which nothing enforces: one account opened mid-history and the fold boundary lands in
the middle of a month, with that month on both sides of it.

## Decision

**`ml/backtest.py` yields folds by period.** A splitter takes the ordered list of
distinct `accounting_period` values and an integer number of folds, and yields
`(train_index, test_index)` pairs where train holds every row whose period is strictly
earlier than the first period of test. Expanding window: fold *k*'s training set is
every period before its test block, so later folds see more history, which is what
`TimeSeriesSplit` does on the axis it understands.

**The test block is one period.** A fold answers the question the product asks — given
everything up to the end of last month, is this month's figure inside its interval — and
a multi-period test block answers a different one.

**`TimeSeriesSplit` is not used.** Not wrapped, not sorted into, not used with a
computed `test_size`.

**Everything fitted is fitted inside the fold.** That includes the cost-centre size band
of `docs/adr/0055`, which is a quantile of the series' own history and is therefore
computed from training rows only.

## Reasoning

**Writing twenty lines rather than arranging for a library to be accidentally correct.**
Sorting by period and computing `test_size` as rows-per-period is the workaround the
documentation suggests, and it works until the panel is ragged. The failure it produces
then is silent — a score that is slightly too good — which is the shape of failure this
repository keeps designing against. A splitter that names periods cannot have that bug,
and it can be asserted on directly.

**Strictly earlier rather than a gap.** `TimeSeriesSplit` offers `gap` to exclude
samples immediately before the test block. It is not used here: the features are lags of
the series' own past, so a row's inputs are periods that are themselves in the training
set, and there is no leakage from adjacency to exclude. A gap would only shorten the
history for no stated reason.

**An expanding window rather than a sliding one.** A sliding window would test whether
the model needs only recent history, which is a tuning question, and `docs/product.md`
puts model research out of scope. The expanding window matches how the thing actually
runs: in production the model is fitted on everything known and asked about the month
just closed.

## Consequences

**Leakage is assertable, and it is asserted.** The gate is that for every fold, the
maximum period in the training set is strictly less than the minimum period in the test
set. That is a property of the splitter, checkable without fitting anything, and it is
one of the behaviours `docs/adr/0054` gates.

**The number of folds is bounded by the history.** With lag-12 in the feature set the
first usable period is the thirteenth, so a 36-period range leaves 24 rows per series to
split, and the fold count is a var with room under it rather than a number at its limit.

**A run judges with a model fitted on everything before the period being judged**, which
is the last fold's shape. Scoring and serving use one code path rather than two that
could drift, and it is a named one: `features.for_period` takes the balances and a
period and returns the design with its training and test rows, or nothing when the
period has no history in front of it. They had drifted once already while both were
"obviously" deriving the same window - the backtest took it from the design's periods,
which begin at the thirteenth, and serving from the raw balances, which begin at the
first. The size band is a tercile over that window, so the two were fitting different
features and the backtest was scoring something production does not do.
