"""Accounts, cost centres and exchange rates.

These are the small tables — hundreds of rows — so they are built in memory. The
entries are not; see `entries.py`.
"""

from datetime import date, timedelta
from decimal import Decimal

from . import schema
from .streams import COST_CENTRE_MOVE, DIMENSIONS, stream_for

BASE_CURRENCY = "CNY"
CURRENCIES = ("CNY", "EUR", "USD", "GBP")

# A rate is drawn at the resolution it is written at - an integer number of
# millionths - and stays integral until one exact division. No float participates,
# for the reason amounts draw whole cents: a float has no exact decimal value, and
# once the pipeline multiplies amounts by rates, where the rounding happened becomes
# a question somebody has to answer. See docs/adr/0013-a-rate-is-not-an-amount.md.
RATE_MIN_MICROS = 5_000_000   # 5.000000
RATE_MAX_MICROS = 9_000_000   # 9.000000, exclusive
DRIFT_PPM = 20_000            # the daily jitter, +/- 2 per cent in parts per million

ACCOUNT_TYPES = ("asset", "liability", "equity", "revenue", "expense")

# Enough breadth for the anomalies to hide in, small enough to read.
ACCOUNT_COUNT = 40
COST_CENTRE_COUNT = 12

# Reserved for the growth anomaly. Keeping the planted growth off the ordinary
# accounts stops it leaking into their monthly totals. They are declared here, and
# emitted whether or not the switch is on, so turning it on does not change
# dim_account_src - and so the entries it plants are never orphans.
GROWTH_DEBIT_ACCOUNT = "6998"
GROWTH_CREDIT_ACCOUNT = "6999"
# Reserved for the long-tail anomaly. Unlike the growth accounts these carry entries
# whether or not the switch is on: the anomaly raises their amounts, and a steady
# entry count is the shape's diagnostic feature.
LONG_TAIL_DEBIT_ACCOUNT = "6996"
LONG_TAIL_CREDIT_ACCOUNT = "6995"

RESERVED_ACCOUNTS = (
    GROWTH_DEBIT_ACCOUNT,
    GROWTH_CREDIT_ACCOUNT,
    LONG_TAIL_DEBIT_ACCOUNT,
    LONG_TAIL_CREDIT_ACCOUNT,
)

EPOCH = date(2020, 1, 1)


def accounts(seed: int) -> list[dict[str, object]]:
    rng = stream_for(seed, DIMENSIONS)
    rows = []
    for index in range(ACCOUNT_COUNT):
        code = f"6{index + 100:03d}"
        parent = f"6{(index // 10) * 10 + 100:03d}"
        rows.append(
            {
                "account_code": code,
                "name": f"account {code}",
                "parent_code": "" if parent == code else parent,
                "account_type": ACCOUNT_TYPES[index % len(ACCOUNT_TYPES)],
                "effective_date": schema.format_date(EPOCH),
            }
        )
    for code in RESERVED_ACCOUNTS:
        rows.append({
            "account_code": code,
            "name": f"account {code}",
            "parent_code": "",
            "account_type": "expense",
            "effective_date": schema.format_date(EPOCH),
        })
    rng.shuffle(rows)
    rows.sort(key=lambda row: row["account_code"])
    return rows


def cost_centres(seed: int, move: bool) -> list[dict[str, object]]:
    """One row per cost centre, unless `move` is on.

    The move is what makes a point-in-time join necessary: a March transaction has to
    be reported against the department the cost centre belonged to in March, not the
    one it belongs to now.
    """
    rng = stream_for(seed, DIMENSIONS)
    rows = []
    for index in range(COST_CENTRE_COUNT):
        code = f"CC-{index + 1:03d}"
        rows.append(
            {
                "cc_code": code,
                "name": f"cost centre {code}",
                "dept_code": f"DEPT-{index % 4 + 1}",
                "effective_date": schema.format_date(EPOCH),
            }
        )
    rng.shuffle(rows)
    rows.sort(key=lambda row: row["cc_code"])

    if move:
        # A separate stream, so turning this on does not disturb anything above.
        move_rng = stream_for(seed, COST_CENTRE_MOVE)
        moved = rows[move_rng.randrange(len(rows))]
        successor = dict(moved)
        successor["dept_code"] = f"DEPT-{(int(moved['dept_code'][-1]) % 4) + 1}"
        successor["effective_date"] = schema.format_date(date(2026, 7, 1))
        rows.append(successor)
        rows.sort(key=lambda row: (row["cc_code"], row["effective_date"]))

    return rows


def _rate(micros: int) -> Decimal:
    """Millionths to a rate. The division is exact, so the quantize fixes the width
    and never rounds. Mirrors `_cents` in entries.py."""
    return (Decimal(micros) / schema.RATE_MICROS).quantize(schema.RATE_PLACES)


def fx_rates(seed: int, months: list[date]) -> list[dict[str, object]]:
    """A rate per currency per day of the generated range."""
    rng = stream_for(seed, DIMENSIONS)
    rows = []
    for currency in CURRENCIES:
        centre = (
            schema.RATE_MICROS
            if currency == BASE_CURRENCY
            else rng.randrange(RATE_MIN_MICROS, RATE_MAX_MICROS)
        )
        for month in months:
            day = month
            while day.month == month.month:
                # Drawn for every currency, including the base, whose value is then
                # discarded. CNY is first in CURRENCIES, so dropping these draws would
                # shift EUR, USD and GBP for a reason unrelated to precision.
                drift = rng.randrange(-DRIFT_PPM, DRIFT_PPM + 1)
                micros = (
                    schema.RATE_MICROS
                    if currency == BASE_CURRENCY
                    else centre * (schema.RATE_MICROS + drift) // schema.RATE_MICROS
                )
                rows.append(
                    {
                        "currency": currency,
                        "rate_date": schema.format_date(day),
                        "rate_to_base": schema.format_rate(_rate(micros)),
                    }
                )
                day += timedelta(days=1)
    return rows
