"""The two arms, and the three quantiles each of them fits.

The interval is fitted rather than added around a point prediction, so an arm is three
estimators and not one: a lower bound, a median, and an upper bound. That is the whole
argument of docs/adr/0052 - a residual band is one width for every series, and one width
is wrong in a busy season and wrong on a quiet one at the same time.

The linear arm is `QuantileRegressor` rather than `Ridge`. `Ridge` fits a conditional
mean and produces no interval at all, so keeping it would have meant bolting a band onto
it and losing the property the layer exists for.
"""

from dataclasses import dataclass

__all__ = ["QUANTILES", "NOMINAL_COVERAGE", "ARMS", "Prediction", "Arm", "build"]

# Lower, median, upper. The nominal coverage is the gap between the outer two, and it is
# the only number a person chooses here: a statement about how many rows are expected to
# fall outside, not a threshold on any amount.
QUANTILES = (0.05, 0.50, 0.95)
NOMINAL_COVERAGE = 0.90

ARMS = ("quantile_linear", "gradient_boosting")


@dataclass(frozen=True)
class Prediction:
    """Three aligned series: one row of each per row of X."""

    lower: list[float]
    median: list[float]
    upper: list[float]


class Arm:
    """One family, fitted at three quantiles.

    `fit` returns self so a caller can write `build(name).fit(X, y).predict(X)`; the
    estimators are held in quantile order so `predict` can hand back a named triple
    rather than a list the caller has to index correctly.
    """

    def __init__(self, name: str, factory):
        self.name = name
        self.quantiles = QUANTILES
        self._factory = factory
        self._fitted = []

    def fit(self, X, y) -> "Arm":
        self._fitted = [self._factory(quantile).fit(X, y) for quantile in self.quantiles]
        return self

    def predict(self, X) -> Prediction:
        if not self._fitted:
            raise RuntimeError(f"{self.name} has not been fitted")
        lower, median, upper = (list(one.predict(X)) for one in self._fitted)
        return Prediction(lower=lower, median=median, upper=upper)


def build(name: str, *, random_state: int = 0) -> Arm:
    """One arm by name. An unknown name is refused rather than defaulted."""
    if name == "quantile_linear":
        from sklearn.linear_model import QuantileRegressor

        # `alpha` is the L1 penalty, small rather than zero: the three lag features are
        # strongly correlated - they are the same series at different offsets - and an
        # unpenalised fit on collinear columns moves its coefficients a long way for a
        # prediction that barely changes. `solver="highs"` is scipy's linear programme;
        # the default was deprecated in favour of naming one.
        return Arm(name, lambda q: QuantileRegressor(
            quantile=q, alpha=0.001, solver="highs",
        ))

    if name == "gradient_boosting":
        from sklearn.ensemble import GradientBoostingRegressor

        return Arm(name, lambda q: GradientBoostingRegressor(
            loss="quantile", alpha=q, random_state=random_state,
        ))

    raise ValueError(f"no arm {name!r}; there is {list(ARMS)}")
