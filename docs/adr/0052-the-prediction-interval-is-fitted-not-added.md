# The prediction interval is fitted, not added around a point prediction

summary: Both arms fit three quantiles — 0.05, 0.50, 0.95 — so the interval is a
conditional quantity the model produces, and the linear arm is `QuantileRegressor`
rather than `Ridge`.

## Context

A balance is flagged when the actual value falls outside a prediction interval. Where
that interval comes from decides what the whole layer is worth, because the argument
against a fixed multiple of the standard deviation is not that it is imprecise — it is
that one band for every series is wrong in both directions at once. It fires in a busy
season on figures that were always going to be high, and it never fires on a series
drifting upward a few percent a month, because the band drifts with it.

`ml/README.md` wrote the design down as "a ridge baseline compared against gradient
boosting". scikit-learn's `Ridge` fits a conditional mean and does not produce an
interval of any kind — verified against the scikit-learn documentation, which for
prediction intervals points at `GradientBoostingRegressor` with `loss="quantile"`.
A ridge baseline therefore needs an interval from somewhere else, and the obvious
somewhere is the spread of its own backtest residuals.

## Decision

**Both arms fit the same three quantiles: 0.05, 0.50 and 0.95.**

| arm | estimator | how the quantile is asked for |
|---|---|---|
| linear | `sklearn.linear_model.QuantileRegressor` | `quantile=` |
| trees | `sklearn.ensemble.GradientBoostingRegressor` | `loss="quantile"`, `alpha=` |

Three fits per arm. The 0.50 fit is the point prediction, the other two are the interval
bounds, and no arm derives a bound from residuals.

**The nominal coverage is 90%**, and it is the only number a person chooses. It is a
statement about how many rows are expected to fall outside — roughly one in ten — rather
than a threshold on any amount.

**The linear arm is `QuantileRegressor`, and `ml/README.md`'s "ridge baseline" wording
is changed to match.** The baseline remains a regularised linear model over the same
features; what changes is the loss it minimises, from squared error to pinball.

**Both arms are scored with `mean_pinball_loss` at each quantile**, plus realised
coverage against the nominal 90% and mean interval width. One instrument, both arms,
which is what "comparable" in the acceptance means.

## Reasoning

**A residual band is a constant, and a constant is the thing being argued against.**
Point prediction plus the 5th and 95th percentiles of the backtest residuals gives every
row in every series the same interval width. It is a better fixed threshold than three
standard deviations — the point prediction carries the trend and the seasonality, so the
band is centred correctly — but the width still says the same thing about a volatile
series and a quiet one. A fitted quantile widens where the data is dispersed and narrows
where it is not, which is the half of the problem a residual band does not address.

**The wording in `ml/README.md` was a design note, not a constraint.** Keeping `Ridge`
to honour a sentence, and bolting an interval onto it, would preserve the word "ridge"
at the cost of the property the layer exists for. The README is a statement of intent
and this decision supersedes that part of it; the part it does not supersede — a
regularised linear baseline against gradient boosting, backtested on a time-series split
— stands.

**A baseline that is a real contender.** `QuantileRegressor` fits by linear programming
with an L1 penalty, so the linear arm keeps what made a linear baseline worth having:
few parameters, coefficients a person can read, and no capacity to memorise. If it is
not beaten by several hundred trees, that is a result worth having rather than a failed
experiment — it says the structure in these series is additive, and nothing read-only
and opaque needs to go into the pipeline.

**Symmetric quantiles rather than a one-sided test.** A balance far below its prediction
is as much a candidate for review as one far above — an expense account that stopped
posting is a real failure mode — so the interval is two-sided and the flag records which
side was crossed.

**90% rather than a tighter nominal.** `docs/产品文档.md` designs for a queue of 200–400
rows per period and says in as many words that the figure is a placeholder rather than a
measurement, and that it must be measured before it becomes a threshold. So the nominal
is chosen for the property it states — one row in ten is expected outside, which is what
makes the rate a stated expectation rather than a discovered one — and the resulting
queue size is reported by the backtest rather than tuned toward that range. On this
repository's synthetic ledger the panel is roughly 250 series, so the queue is tens of
rows per period and not hundreds; the product figure describes a company, not this data.

## Consequences

**Three fits per arm, six per backtest fold.** Cheap at this panel's size and worth
naming, because it is the reason a fold is not free.

**The quantiles can cross, and a crossed interval is counted rather than repaired.**
Nothing constrains the 0.05 fit to stay below the 0.95 fit, and quantile regressions
fitted independently sometimes cross on rows far from the data. Such a row produces no
flag — the interval it would be tested against is not one — and it is counted. The
counter lives in two places and in neither case on a flag row, because a row that is not
flagged has no flag row to carry it: `judge` reports `intervals_crossed` in the step
detail it returns to the run record, and the backtest artefact reports it per fold and
per arm. Reordering the bounds silently would hide exactly the rows where the fit is
least trustworthy, and an arm that crosses often is a finding about that arm.

**A zero-width interval is the same case.** `score` normalises the excess by the
interval width, so bounds that coincide would divide by zero. A row whose bounds are
equal is treated as crossed: not flagged, counted. It means the fit has no dispersion to
offer there, which is not a basis on which to put a row in an analyst's queue.

**Realised coverage is a reported number, not a gate.** See `docs/adr/0054`.

**`ml/README.md` changes with this decision**, rather than being left to disagree with
the code. The same applies to `docs/architecture.md`'s one-line description of `ml/`.
