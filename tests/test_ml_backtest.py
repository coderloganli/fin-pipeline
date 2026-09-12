"""The backtest: how folds are cut, and what the two arms are scored with.

Cases 8-14, 17 and 37 of task.md.

docs/adr/0053 is why the splitter is written here rather than taken from scikit-learn:
`TimeSeriesSplit` splits contiguous row positions and this is a panel of many series
over the same months, so a positional cut falls between series rather than between
periods. docs/adr/0052 is why both arms fit three quantiles rather than one arm fitting
a mean and having a band added to it.
"""

import random
from decimal import Decimal

import pytest

from ml import backtest, features, models
# pytest puts the test directory on sys.path, so this is a plain module import - the
# same shape tests/test_long_tail.py uses to reuse test_generator's predicates.
from test_ml_features import THIRTY_SIX, balances


def design_for(series: dict, span=None):
    return features.build(balances(series, span=span))


FLAT_PANEL = {
    ("660204", "CC01"): list(range(1, 37)),
    ("660104", "CC02"): list(range(100, 136)),
}

# Scoring needs a panel a model cannot fit exactly: with no residual left over, all
# three quantiles land on the same line and every interval is zero width. The splitter
# cases above do not fit anything, so they keep the flat one.
_NOISE = random.Random(20260912)
NOISY_PANEL = {
    ("660204", "CC01"): [500 + _NOISE.randrange(-120, 121) for _ in range(36)],
    ("660104", "CC02"): [4000 + _NOISE.randrange(-900, 901) for _ in range(36)],
}


# --- case 8 -----------------------------------------------------------------

def test_no_fold_trains_on_a_period_it_tests():
    """Case 8. The gate the whole splitter exists for, and it is checkable without
    fitting anything: a fold that trained on the period it is judging would report a
    score that describes nothing."""
    design = design_for(FLAT_PANEL)

    folds = list(backtest.folds(design, n_splits=5))
    assert folds, "the splitter yielded nothing"

    for fold in folds:
        trained = {design.periods[i] for i in fold.train_index}
        tested = {design.periods[i] for i in fold.test_index}
        assert trained and tested
        assert max(trained) < min(tested), (
            f"fold trains through {max(trained)} and tests {min(tested)}"
        )


# --- case 9 -----------------------------------------------------------------

def test_a_fold_tests_exactly_one_period():
    """Case 9. A fold answers the question the product asks - given everything up to
    the end of last month, is this month inside its interval - and a multi-period test
    block answers a different one."""
    design = design_for(FLAT_PANEL)

    for fold in backtest.folds(design, n_splits=5):
        tested = {design.periods[i] for i in fold.test_index}
        assert len(tested) == 1, f"fold tested {sorted(tested)}"


# --- case 10 ----------------------------------------------------------------

def test_the_window_expands():
    """Case 10. Each fold's training set is a superset of the one before: that is what
    `TimeSeriesSplit` does on the axis it understands, and what production does - the
    model is fitted on everything known and asked about the month just closed."""
    folds = list(backtest.folds(design_for(FLAT_PANEL), n_splits=5))

    for earlier, later in zip(folds, folds[1:]):
        assert set(earlier.train_index) < set(later.train_index)


# --- case 11 ----------------------------------------------------------------

def test_a_ragged_panel_does_not_put_one_period_on_both_sides():
    """Case 11. The failure a positional split produces. Series B starts late, so the
    panel has a different number of rows per period, and a splitter computing a row
    count per period would cut inside a month - with that month on both sides of the
    fold, and a score slightly too good to notice."""
    span = THIRTY_SIX
    rows = balances({("660204", "A"): list(range(1, 37))}, span=span)
    rows += balances({("660104", "B"): list(range(1, 17))}, span=span[20:])
    design = features.build(rows)

    for fold in backtest.folds(design, n_splits=3):
        trained = {design.periods[i] for i in fold.train_index}
        tested = {design.periods[i] for i in fold.test_index}
        assert len(tested) == 1
        assert max(trained) < min(tested)


# --- case 12 ----------------------------------------------------------------

def test_asking_for_more_folds_than_there_are_periods_is_refused():
    """Case 12. Not silently fewer folds. A backtest that quietly halved its own
    coverage would report a number over a range nobody chose."""
    design = design_for(FLAT_PANEL)
    available = len(set(design.periods))

    with pytest.raises(ValueError) as failure:
        list(backtest.folds(design, n_splits=available + 5))
    assert str(available) in str(failure.value)


# --- case 13 ----------------------------------------------------------------

@pytest.mark.parametrize("arm", ["quantile_linear", "gradient_boosting"])
def test_both_arms_fit_three_quantiles(arm):
    """Case 13. The interval is fitted rather than added: a lower bound, a median and
    an upper bound, each its own fit. See docs/adr/0052."""
    design = design_for(FLAT_PANEL)
    fitted = models.build(arm, random_state=0).fit(design.X, design.y)

    assert tuple(fitted.quantiles) == (0.05, 0.50, 0.95)

    prediction = fitted.predict(design.X)
    assert len(prediction.lower) == len(prediction.median) == len(prediction.upper)
    assert len(prediction.lower) == len(design.X)


# --- case 14 ----------------------------------------------------------------

@pytest.mark.parametrize("arm", ["quantile_linear", "gradient_boosting"])
def test_the_interval_widens_where_the_data_is_dispersed(arm):
    """Case 14. The property that separates a fitted interval from a residual band,
    and the reason a fixed multiple of the standard deviation was rejected: one band
    for every series is wrong in a busy season and wrong on a quiet one.

    A is constant, so it has no dispersion to explain; B sits ten times higher and
    carries noise no feature can predict. A band added around a point prediction would
    give them the same width. The noise is drawn from a fixed seed, so this asserts a
    property of the estimator rather than of a lucky draw.
    """
    rng = random.Random(20260911)
    panel = {
        ("660204", "A"): [100] * 36,
        ("660104", "B"): [5000 + rng.randrange(-2000, 2001) for _ in range(36)],
    }
    design = design_for(panel)
    fitted = models.build(arm, random_state=0).fit(design.X, design.y)
    prediction = fitted.predict(design.X)

    widths = {"A": [], "B": []}
    for index, row in enumerate(design.rows):
        widths[row.key[1]].append(prediction.upper[index] - prediction.lower[index])

    mean_a = sum(widths["A"]) / len(widths["A"])
    mean_b = sum(widths["B"]) / len(widths["B"])
    assert mean_b > mean_a, f"constant series got width {mean_a}, noisy series {mean_b}"


def test_a_non_positive_split_count_is_refused():
    """Case 12, the other end of it. `periods[-0:]` is the whole list, so a zero would
    silently backtest every period against an empty training set instead of saying
    the number made no sense."""
    for bad in (0, -1):
        with pytest.raises(ValueError, match="at least 1"):
            list(backtest.folds(design_for(FLAT_PANEL), n_splits=bad))


def test_every_fold_builds_its_own_design_from_its_own_history():
    """Case 8's other half, and the one an assertion about periods cannot catch.

    The size band is a tercile of each cost centre's median over the training periods,
    so a matrix built once and sliced would carry bands computed from periods the fold
    is supposed not to have seen - leakage through a side channel, with nothing about
    the split wrong. `features.for_period` is the seam both scoring and serving go
    through; this asserts the backtest goes through it once per fold rather than
    preparing the data once.
    """
    import ml.backtest as module

    seen = []
    real = features.for_period

    def watch(rows, period, **kwargs):
        seen.append(period)
        return real(rows, period, **kwargs)

    monkey = type("Patched", (), {
        "for_period": staticmethod(watch),
        "build": staticmethod(features.build),
        "Balance": features.Balance,
    })
    original, module.features = module.features, monkey
    try:
        backtest.run(balances(FLAT_PANEL), n_splits=3, random_state=0)
    finally:
        module.features = original

    # Two arms over three folds: one preparation per arm per fold, three distinct
    # target periods. One distinct period would mean the data was prepared once.
    assert len(seen) == 6, f"expected a preparation per arm per fold, got {len(seen)}"
    assert len(set(seen)) == 3, f"folds shared a preparation: {sorted(set(seen))}"


def test_a_folds_training_rows_all_precede_its_target_period():
    """The same property asserted on the seam itself rather than through `run`: what
    `features.for_period` hands back for a period never contains a training row at or
    after it, so nothing fitted on it can have seen the period being judged."""
    rows = balances(FLAT_PANEL)
    design, train, test = features.for_period(rows, THIRTY_SIX[30])

    assert {design.periods[i] for i in test} == {THIRTY_SIX[30]}
    assert max(design.periods[i] for i in train) < THIRTY_SIX[30]


def test_a_period_with_no_history_in_front_of_it_prepares_nothing():
    """`for_period` answers None rather than an empty design, so a caller has to decide
    what to do about it. `judge` reports the period as not judged; `backtest` skips the
    fold. Both are deliberate, and neither can happen by accident on an empty list."""
    rows = balances(FLAT_PANEL)

    assert features.for_period(rows, THIRTY_SIX[0]) is None


# --- case 17 ----------------------------------------------------------------

def test_both_arms_are_scored_with_the_same_instrument():
    """Case 17. "Comparable" in the acceptance means one instrument, both arms - not
    two numbers that happen to be floats.

    A panel with noise in it rather than `FLAT_PANEL`. A perfectly linear series is
    fitted exactly by all three quantiles at once, so every interval comes out zero
    width and the coverage of nothing is not a coverage - which says something about
    the fixture and nothing about whether the two arms are scored alike. Noise is drawn
    from a fixed seed, so the numbers are still the same on every run.
    """
    report = backtest.run(balances(NOISY_PANEL), n_splits=3, random_state=0)

    assert set(report.arms) == {"quantile_linear", "gradient_boosting"}
    for name, scored in report.arms.items():
        assert set(scored.pinball) == {0.05, 0.50, 0.95}, name
        assert scored.coverage is not None, name
        assert scored.mean_interval_width is not None, name
        assert scored.mae is not None, name


# --- case 37 ----------------------------------------------------------------

def test_the_artefact_is_written_where_a_person_can_read_it(tmp_path):
    """Case 37. docs/adr/0054 says the scores are recorded rather than gated, which
    means nothing unless they are somewhere findable. A number printed during CI is
    gone by the time anybody asks what the two arms scored."""
    import json

    report = backtest.run(balances(NOISY_PANEL), n_splits=3, random_state=0)
    written = backtest.write(report, tmp_path / "ml" / "backtest.json")

    assert written.is_file()
    artefact = json.loads(written.read_text(encoding="utf-8"))
    assert set(artefact["arms"]) == {"quantile_linear", "gradient_boosting"}


def test_the_artefact_carries_both_arms_fold_by_fold():
    """Case 37. Acceptance item 2 is satisfied by something a person can read, so it is
    written down rather than printed and lost. Scores are recorded and not
    thresholded - see docs/adr/0054."""
    report = backtest.run(balances(NOISY_PANEL), n_splits=3, random_state=0)
    artefact = report.as_dict()

    assert artefact["nominal_coverage"] == 0.90
    for name in ("quantile_linear", "gradient_boosting"):
        arm = artefact["arms"][name]
        assert len(arm["folds"]) == 3
        for fold in arm["folds"]:
            for field in ("period", "coverage", "mean_interval_width",
                          "pinball", "mae", "flagged", "intervals_crossed"):
                assert field in fold, f"{name} fold is missing {field}"
