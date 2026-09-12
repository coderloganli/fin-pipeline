"""Feature construction: every feature comes from the series' own past.

Cases 1-7 of task.md. See docs/adr/0055: lags of 1, 3 and 12 periods off the dense
grid, a month indicator, and a cost-centre size band computed from training rows only.
Nothing is imputed, so a series contributes no row until its thirteenth period.

These tests need neither Postgres nor Spark: `ml.features` is handed balances and gives
back a design matrix, and reading the mart is somebody else's job.
"""

from decimal import Decimal

import pytest

from ml import features


def periods(count: int, *, start_year: int = 2024) -> list[str]:
    """`count` consecutive monthly periods, beginning at January of `start_year`."""
    out = []
    year, month = start_year, 1
    for _ in range(count):
        out.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return out


THIRTY_SIX = periods(36)


def balances(series: dict[tuple[str, str], list], span: list[str] | None = None):
    """A dense panel. `series` maps (account, cost centre) to one value per period."""
    span = span or THIRTY_SIX
    rows = []
    for (account, centre), values in series.items():
        assert len(values) == len(span), "the grid is dense: one value per period"
        for period, value in zip(span, values):
            rows.append(features.Balance(
                account_code=account,
                cost_center_code=centre,
                accounting_period=period,
                balance=Decimal(str(value)),
            ))
    return rows


# --- case 1 -----------------------------------------------------------------

def test_a_lag_is_the_row_that_many_periods_back():
    """Case 1. Balance equals the period's ordinal, so a lag's value names the period
    it came from and an off-by-one cannot pass."""
    design = features.build(balances({("660204", "CC01"): list(range(1, 37))}))

    row = design.row("660204", "CC01", THIRTY_SIX[12])   # the 13th period
    assert row["lag_1"] == pytest.approx(12.0)
    assert row["lag_3"] == pytest.approx(10.0)
    assert row["lag_12"] == pytest.approx(1.0)


# --- case 2 -----------------------------------------------------------------

def test_a_series_contributes_nothing_until_its_thirteenth_period():
    """Case 2. Twelve periods of history are consumed before a row exists, and the
    earliest row is the thirteenth period rather than the first."""
    design = features.build(balances({("660204", "CC01"): list(range(1, 37))}))

    assert len(design.rows) == 24
    assert min(design.periods) == THIRTY_SIX[12]


# --- case 3 -----------------------------------------------------------------

def test_a_period_with_no_activity_is_a_zero_and_not_a_gap():
    """Case 3. The grid is dense (docs/adr/0033), so the previous period is the
    previous period. A sparse table would make `lag_1` the last month that had
    activity, silently, and a comparison against it would be a comparison with
    something else."""
    values = list(range(1, 37))
    values[4] = 0                                  # the 5th period posted nothing
    design = features.build(balances({("660204", "CC01"): values}))

    row = design.row("660204", "CC01", THIRTY_SIX[13])   # the 14th, whose lag_12 is 2
    assert row["lag_12"] == pytest.approx(2.0)

    # And the zero reaches the feature as a zero: the 6th period's lag_1.
    design_early = features.build(
        balances({("660204", "CC01"): values}), max_lag=1,
    )
    assert design_early.row("660204", "CC01", THIRTY_SIX[5])["lag_1"] == pytest.approx(0.0)


# --- case 4 -----------------------------------------------------------------

def test_nothing_is_imputed_and_the_row_count_says_so():
    """Case 4. No NaN anywhere, and exactly (periods - 12) rows per series. An
    imputed lag would be an invented figure in a repository whose argument is that
    zero and null are different facts."""
    design = features.build(balances({
        ("660204", "CC01"): list(range(1, 37)),
        ("660104", "CC02"): list(range(100, 136)),
    }))

    assert len(design.rows) == (36 - 12) * 2
    for row in design.X:
        for value in row:
            assert value == value, "NaN reached the design matrix"
            assert value is not None


# --- case 5 -----------------------------------------------------------------

def test_the_size_band_is_computed_from_the_training_periods_only():
    """Case 5. The band is a tercile of each cost centre's median balance, and a band
    computed over the whole panel would be computed partly from periods the fold is
    supposed not to have seen - leakage through a side channel, which no assertion
    about the split would catch.

    A is small for its first twelve periods and large for the remaining twenty-four,
    so its median moves from the bottom of the three cost centres to the top depending
    on which periods were used to compute it.
    """
    panel = balances({
        ("660204", "A"): [100] * 12 + [1000] * 24,
        ("660204", "B"): [500] * 36,
        ("660204", "C"): [900] * 36,
    })
    judged = THIRTY_SIX[19]

    early = features.build(panel, training_periods=THIRTY_SIX[:12])
    whole = features.build(panel, training_periods=THIRTY_SIX)

    assert early.row("660204", "A", judged)["size_band"] == pytest.approx(0.0)
    assert whole.row("660204", "A", judged)["size_band"] == pytest.approx(2.0)


# --- case 6 -----------------------------------------------------------------

def test_a_cost_centre_absent_from_training_falls_in_the_middle_band():
    """Case 6. Not the bottom band: a cost centre nobody has seen is not thereby small,
    and putting it at an end would make the band say something the data did not."""
    panel = balances({
        ("660204", "A"): [100] * 36,
        ("660204", "B"): [500] * 36,
        ("660204", "C"): [900] * 36,
        ("660204", "D"): [0] * 24 + [700] * 12,
    })

    design = features.build(panel, training_periods=THIRTY_SIX[:24], seen_in=THIRTY_SIX[:24])

    assert design.row("660204", "D", THIRTY_SIX[30])["size_band"] == pytest.approx(1.0)


# --- case 7 -----------------------------------------------------------------

def test_the_month_indicator_is_twelve_columns_and_exactly_one_of_them_is_set():
    """Case 7. One-hot, not an integer: a month is not twice January in February, and
    a linear model handed the ordinal would read it as if it were."""
    design = features.build(balances({("660204", "CC01"): list(range(1, 37))}))

    month_columns = [name for name in design.columns if name.startswith("month_")]
    assert len(month_columns) == 12

    for row in design.rows:
        set_columns = [name for name in month_columns if row.values[name] == 1.0]
        assert len(set_columns) == 1, f"{row.key} set {set_columns}"
        expected = int(row.key[2].split("-")[1])
        assert set_columns[0] == f"month_{expected:02d}"
