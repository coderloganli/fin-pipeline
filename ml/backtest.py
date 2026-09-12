"""Cutting the folds, and scoring both arms with one instrument.

The folds are cut on the accounting period by the splitter below rather than by
scikit-learn's `TimeSeriesSplit`, which splits contiguous row positions and has no
notion of a time column. This data is a panel - roughly two hundred and fifty series
over the same months - so a positional cut falls between series, and every fold would
train on one set of cost centres to predict another over the same months. Sorting by
period first makes the cut land correctly only while every period holds exactly the same
number of rows, which nothing enforces. See docs/adr/0053.

Expanding window, one period per test block: a fold answers the question the product
asks - given everything up to the end of last month, is this month inside its interval.
"""

from dataclasses import dataclass, field

from ml import features, models

__all__ = ["Fold", "Scored", "Report", "folds", "run", "write", "main"]


@dataclass(frozen=True)
class Fold:
    """Row positions, and the one period being tested."""

    train_index: list[int]
    test_index: list[int]
    period: str


@dataclass
class Scored:
    """One arm over the whole backtest."""

    coverage: float | None = None
    mean_interval_width: float | None = None
    mae: float | None = None
    pinball: dict = field(default_factory=dict)
    folds: list = field(default_factory=list)


@dataclass
class Report:
    nominal_coverage: float
    arms: dict

    def as_dict(self) -> dict:
        """The artefact. Acceptance item 2 is satisfied by something a person reads, so
        it is written down rather than printed - and nothing in it is thresholded, which
        is the decision docs/adr/0054 records."""
        return {
            "nominal_coverage": self.nominal_coverage,
            "arms": {
                name: {
                    "coverage": scored.coverage,
                    "mean_interval_width": scored.mean_interval_width,
                    "mae": scored.mae,
                    "pinball": scored.pinball,
                    "folds": scored.folds,
                }
                for name, scored in self.arms.items()
            },
        }


def folds(design, *, n_splits: int = 5):
    """Folds cut on the period, newest last.

    Train is every row whose period is strictly earlier than the test period. Strictly,
    with no gap: the features are lags of the series' own past, so a row's inputs are
    periods already in the training set and there is nothing adjacent to exclude.
    """
    periods = sorted(set(design.periods))
    if n_splits < 1:
        # `periods[-0:]` is the whole list, so a zero would quietly backtest every
        # period against an empty training set rather than refusing.
        raise ValueError(f"n_splits must be at least 1, got {n_splits}")
    # One period has to remain for training under the earliest test block, so the number
    # of test blocks available is one fewer than the number of periods.
    available = len(periods)
    if n_splits >= available:
        raise ValueError(
            f"n_splits={n_splits} needs more history than there is: the design covers "
            f"{available} periods, and a fold has to leave at least one period to train "
            f"on"
        )

    for period in periods[-n_splits:]:
        train = [i for i, one in enumerate(design.periods) if one < period]
        test = [i for i, one in enumerate(design.periods) if one == period]
        if not train or not test:
            continue
        yield Fold(train_index=train, test_index=test, period=period)


def _pinball(actual: float, predicted: float, quantile: float) -> float:
    """scikit-learn's `mean_pinball_loss` over one observation, written out because it
    is three lines and the arrays here are lists rather than frames."""
    delta = actual - predicted
    return quantile * delta if delta > 0 else (quantile - 1) * delta


def _take(values, index):
    return [values[i] for i in index]


def run(balances, *, n_splits: int = 5, random_state: int = 0,
        arms=models.ARMS) -> Report:
    """Both arms over every fold, scored with the same instrument.

    Takes balances rather than a design matrix, because everything fitted has to be
    fitted inside the fold and the size band is fitted: it is a tercile of each cost
    centre's median over the training periods. A matrix built once and sliced would
    carry bands computed from periods the fold is supposed not to have seen - leakage
    through a side channel, which the split assertion cannot catch because nothing
    about the split is wrong. See docs/adr/0053 and 0055.
    """
    balances = list(balances)
    # Only to find which periods there are to fold over. Nothing fitted comes off this
    # one - every fold builds its own design through `features.for_period`.
    axis = features.build(balances)
    cut = list(folds(axis, n_splits=n_splits))
    scored = {name: Scored() for name in arms}

    for name in arms:
        totals = {"coverage": [], "width": [], "absolute": []}
        pinball_totals = {q: [] for q in models.QUANTILES}

        for fold in cut:
            prepared = features.for_period(balances, fold.period)
            if prepared is None:
                continue
            design, train, test = prepared

            X_train, y_train = _take(design.X, train), _take(design.y, train)
            X_test, y_test = _take(design.X, test), _take(design.y, test)

            arm = models.build(name, random_state=random_state).fit(X_train, y_train)
            prediction = arm.predict(X_test)

            inside, widths, absolute, crossed, flagged = [], [], [], 0, 0
            per_quantile = {q: [] for q in models.QUANTILES}
            for position, actual in enumerate(y_test):
                lower = prediction.lower[position]
                median = prediction.median[position]
                upper = prediction.upper[position]
                for quantile, predicted in zip(models.QUANTILES, (lower, median, upper)):
                    per_quantile[quantile].append(_pinball(actual, predicted, quantile))
                absolute.append(abs(actual - median))
                if upper <= lower:
                    crossed += 1
                    continue
                widths.append(upper - lower)
                if lower <= actual <= upper:
                    inside.append(1.0)
                else:
                    inside.append(0.0)
                    flagged += 1

            scored[name].folds.append({
                "period": fold.period,
                "coverage": _mean(inside),
                "mean_interval_width": _mean(widths),
                "pinball": {q: _mean(values) for q, values in per_quantile.items()},
                "mae": _mean(absolute),
                "flagged": flagged,
                "intervals_crossed": crossed,
            })
            totals["coverage"].extend(inside)
            totals["width"].extend(widths)
            totals["absolute"].extend(absolute)
            for quantile, values in per_quantile.items():
                pinball_totals[quantile].extend(values)

        scored[name].coverage = _mean(totals["coverage"])
        scored[name].mean_interval_width = _mean(totals["width"])
        scored[name].mae = _mean(totals["absolute"])
        scored[name].pinball = {q: _mean(v) for q, v in pinball_totals.items()}

    return Report(nominal_coverage=models.NOMINAL_COVERAGE, arms=scored)


def _mean(values) -> float | None:
    """None rather than zero for an empty fold: a coverage of nothing is not a coverage
    of zero, and an artefact that said 0.0 would be read as a model that covered
    nothing."""
    return sum(values) / len(values) if values else None


DEFAULT_ARTEFACT = "data/ml/backtest.json"


def write(report: Report, path) -> "Path":
    """The artefact, on disk.

    docs/adr/0054 records that the scores are read rather than gated, which only means
    anything if they are somewhere a person can find. A number printed to a terminal
    during CI is not - it is gone by the time anybody asks what the two arms scored.
    """
    import json
    from pathlib import Path as _Path

    target = _Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(report.as_dict(), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return target


def main(argv=None) -> int:
    """`python -m ml.backtest` - both arms over the mart, written down.

    This is what acceptance item 2 is satisfied by: ridge and gradient boosting with
    comparable numbers, from a time-series split, in a file somebody can open.
    """
    import argparse

    from ml import store
    from transform import db

    parser = argparse.ArgumentParser(description="Backtest both arms over the mart.")
    parser.add_argument("--splits", type=int, default=5)
    parser.add_argument("--out", default=DEFAULT_ARTEFACT)
    parser.add_argument("--random-state", dest="random_state", type=int, default=0)
    args = parser.parse_args(argv)

    values = db.settings()
    with db.connection_from(values) as connection:
        balances = store.read_balances(connection, values["POSTGRES_MART_SCHEMA"])

    report = run(balances, n_splits=args.splits, random_state=args.random_state)
    written = write(report, args.out)

    print(f"backtest over {args.splits} folds, written to {written}")
    for name, scored in report.arms.items():
        print(f"  {name}: coverage={_show(scored.coverage)} "
              f"width={_show(scored.mean_interval_width)} mae={_show(scored.mae)}")
    return 0


def _show(value) -> str:
    return "n/a" if value is None else f"{value:.4f}"


if __name__ == "__main__":
    raise SystemExit(main())
