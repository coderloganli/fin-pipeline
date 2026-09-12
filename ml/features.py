"""The design matrix: every feature comes from the series' own past.

Lags of one, three and twelve periods off the dense monthly grid, a one-hot month, and
a cost-centre size band computed from the training periods only. Nothing is imputed, so
a series contributes no row until it has a full set of lags.

The grid being dense is what makes a lag a lag: docs/adr/0033 gives every active
(account, cost centre) pair a row in every period, zero where nothing posted, so
"twelve periods back" is the row twelve back. Over a sparse table it would silently
become the twelfth most recent period that had activity, and a comparison against that
is a comparison with something else.

See docs/adr/0055.
"""

from dataclasses import dataclass, field

__all__ = ["LAGS", "MONTHS", "SIZE_BAND", "MIDDLE_BAND", "Balance", "Row", "Design",
           "build", "columns_for", "for_period"]

# The lags the model is given. Twelve is the one that costs: it is what a year-ago
# comparison needs, and it is why a range has to be three years rather than one.
LAGS = (1, 3, 12)

MONTHS = tuple(f"month_{month:02d}" for month in range(1, 13))
SIZE_BAND = "size_band"

# A cost centre nobody saw in training is not thereby small. Putting it at an end of the
# range would make the band say something the data did not.
MIDDLE_BAND = 1.0


@dataclass(frozen=True)
class Balance:
    """One cell of the monthly grid, as it comes out of the mart."""

    account_code: str
    cost_center_code: str
    accounting_period: str
    balance: object


@dataclass(frozen=True)
class Row:
    """One row of the design matrix, and the key it was built for."""

    key: tuple[str, str, str]
    values: dict[str, float]


@dataclass
class Design:
    """The matrix, the target, and enough of the keys to say what each row is about."""

    columns: list[str]
    rows: list[Row] = field(default_factory=list)

    @property
    def X(self) -> list[list[float]]:
        """Only the declared columns. `_target` and `_actual` live in the same dict and
        are deliberately not among them - a matrix that carried its own target would
        fit it perfectly and say nothing."""
        return [[row.values[name] for name in self.columns] for row in self.rows]

    @property
    def y(self) -> list[float]:
        return [row.values["_target"] for row in self.rows]

    @property
    def actual(self) -> list:
        """The same targets as the mart holds them, undegraded by the float cast."""
        return [row.values["_actual"] for row in self.rows]

    @property
    def periods(self) -> list[str]:
        return [row.key[2] for row in self.rows]

    def row(self, account_code: str, cost_center_code: str, period: str) -> dict:
        """One row by its key. Raises rather than returning None: a test or a caller
        asking for a row that was never built is asking the wrong question, and a
        silent None would turn that into an assertion about a missing key."""
        wanted = (account_code, cost_center_code, period)
        for row in self.rows:
            if row.key == wanted:
                return row.values
        raise KeyError(
            f"no row for {wanted}; the design holds {len(self.rows)} rows over "
            f"{len(set(self.periods))} periods"
        )


def columns_for(lags) -> list[str]:
    return [f"lag_{lag}" for lag in lags] + list(MONTHS) + [SIZE_BAND]


def _lags_for(max_lag: int) -> tuple[int, ...]:
    return tuple(lag for lag in LAGS if lag <= max_lag)


def _terciles(values: list[float]) -> tuple[float, float]:
    """The two cut points, without pulling numpy in for three numbers."""
    ordered = sorted(values)
    def at(fraction: float) -> float:
        if len(ordered) == 1:
            return ordered[0]
        position = fraction * (len(ordered) - 1)
        low = int(position)
        high = min(low + 1, len(ordered) - 1)
        return ordered[low] + (ordered[high] - ordered[low]) * (position - low)
    return at(1 / 3), at(2 / 3)


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def _bands(rows, training_periods, seen_in) -> dict[str, float]:
    """Each cost centre's band, as terciles of its median balance in training.

    The median rather than the mean, because the median of a series carrying a planted
    anomaly is where the series normally sits: a mean is dragged by the very thing the
    model is being built to detect, and the band would then be partly a function of it.
    """
    history: dict[str, list[float]] = {}
    activity: dict[str, bool] = {}
    for row in rows:
        centre = row.cost_center_code
        if row.accounting_period in training_periods:
            history.setdefault(centre, []).append(float(row.balance))
        if row.accounting_period in seen_in and float(row.balance) != 0.0:
            activity[centre] = True

    medians = {
        centre: _median(values)
        for centre, values in history.items() if activity.get(centre)
    }
    if not medians:
        return {}

    low, high = _terciles(list(medians.values()))
    bands = {}
    for centre, median in medians.items():
        bands[centre] = 0.0 if median <= low else (2.0 if median > high else 1.0)
    return bands


def build(rows, *, training_periods=None, seen_in=None, max_lag: int = max(LAGS)) -> Design:
    """The design matrix over a panel of monthly balances.

    `training_periods` is what anything fitted from the data may look at - today that is
    the size band alone. It defaults to every period present, which is right for serving
    and wrong inside a fold; `backtest` passes the fold's own training periods, because
    a band computed over the whole panel is leakage through a side channel and no
    assertion about the split would catch it.

    `seen_in` is which periods decide whether a cost centre has been seen at all, and
    defaults to `training_periods`. A centre that posted nothing across them has no
    median worth taking and falls in the middle band.
    """
    rows = list(rows)
    everything = sorted({row.accounting_period for row in rows})
    training_periods = set(training_periods if training_periods is not None else everything)
    seen_in = set(seen_in if seen_in is not None else training_periods)

    lags = _lags_for(max_lag)
    bands = _bands(rows, training_periods, seen_in)

    # The grid, keyed so a lag is a lookup rather than a scan.
    grid: dict[tuple[str, str], dict[str, float]] = {}
    exact: dict[tuple[str, str], dict[str, object]] = {}
    for row in rows:
        key = (row.account_code, row.cost_center_code)
        grid.setdefault(key, {})[row.accounting_period] = float(row.balance)
        exact.setdefault(key, {})[row.accounting_period] = row.balance

    design = Design(columns=columns_for(lags))
    for (account, centre), series in sorted(grid.items()):
        ordered = sorted(series)
        for index, period in enumerate(ordered):
            if index < max_lag:
                # No imputation: a filled lag is an invented figure, and zero and null
                # are different facts here. See docs/adr/0033.
                continue
            values = {f"lag_{lag}": series[ordered[index - lag]] for lag in lags}
            month = int(period.split("-")[1])
            values.update({name: 0.0 for name in MONTHS})
            values[f"month_{month:02d}"] = 1.0
            values[SIZE_BAND] = bands.get(centre, MIDDLE_BAND)
            values["_target"] = series[period]
            # The exact figure as the mart holds it, carried alongside the float the
            # estimators need. A flag reports `actual` to an analyst against a report
            # built from Decimal (docs/adr/0013 and 0031), so the number in the queue
            # is the mart's own rather than a float round-trip of it.
            values["_actual"] = exact[(account, centre)][period]
            design.rows.append(Row(key=(account, centre, period), values=values))
    return design


def for_period(balances, period: str, *, max_lag: int = max(LAGS)):
    """The design for judging one period, fitted on everything before it.

    `(design, train_index, test_index)`, or `None` when the period has no history in
    front of it and therefore cannot be judged.

    One function rather than one per caller. `backtest` and `judge` both need this and
    they had derived it separately, off different period lists - the backtest took its
    training window from the design's periods, which begin at the thirteenth, and
    serving took it from the raw balances, which begin at the first. The size band is a
    tercile over that window, so the two were fitting different features and the
    backtest was scoring something production does not do. `docs/adr/0053` asks for one
    code path for scoring and serving; this is it.
    """
    training_periods = sorted(
        {row.accounting_period for row in balances if row.accounting_period < period}
    )
    if not training_periods:
        return None

    design = build(balances, training_periods=training_periods, max_lag=max_lag)
    train = [i for i, one in enumerate(design.periods) if one < period]
    test = [i for i, one in enumerate(design.periods) if one == period]
    if not train or not test:
        return None
    return design, train, test
