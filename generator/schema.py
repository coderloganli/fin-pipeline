"""Column names, order and formatting for the five source tables.

This module is the single truth for what the generator emits. Writers take their
column order from here and tests assert against these constants, so an implementation
cannot move the target and stay green.

What ingest *expects* is a separate statement, declared later under
`ingest/contracts/`. Keeping the two apart is the point: a contract that is derived
from the producer cannot catch the producer changing.
"""

from datetime import date, datetime, timedelta
from decimal import Decimal

DATE_FORMAT = "%Y-%m-%d"
AMOUNT_PLACES = Decimal("0.01")

# A rate is not a currency amount, and does not share its width. Two decimal places
# on a rate near 7.87 is a 0.06% error on every conversion, and one that a rerun
# reproduces faithfully rather than exposing. The contract's `min: 0.000001` already
# commits to six places; this agrees with it rather than choosing a second number.
# See docs/adr/0013-a-rate-is-not-an-amount.md.
RATE_PLACES = Decimal("0.000001")
RATE_MICROS = 1_000_000   # millionths of a unit: the resolution a rate is drawn at

COLUMNS: dict[str, tuple[str, ...]] = {
    "gl_entry": (
        "entry_id",
        "version",
        "accounting_date",
        "posted_at",
        "account_code",
        "cost_center_code",
        "currency",
        "amount_dr",
        "amount_cr",
        "doc_id",
    ),
    "gl_adjustment": (
        "entry_id",
        "version",
        "accounting_date",
        "posted_at",
        "account_code",
        "cost_center_code",
        "currency",
        "amount_dr",
        "amount_cr",
        "doc_id",
        "adjusts_entry_id",
        "adjustment_type",
    ),
    "dim_account_src": (
        "account_code",
        "name",
        "parent_code",
        "account_type",
        "effective_date",
    ),
    "dim_cost_center_src": (
        "cc_code",
        "name",
        "dept_code",
        "effective_date",
    ),
    "fx_rate": (
        "currency",
        "rate_date",
        "rate_to_base",
    ),
}

# The column `schema_drift` adds or removes. It is a real column rather than a
# throwaway one so that the drift a later contract check sees is the kind that
# matters: a field with meaning appearing or disappearing.
DRIFT_COLUMN = "source_system"
DRIFT_VALUE = "erp-main"

# Which column `drop_column` removes, per table. Named explicitly rather than "the
# last one": the last column of gl_adjustment is `adjustment_type` and of
# dim_cost_center_src is `effective_date`, so dropping blindly would delete the very
# signal another switch exists to produce, and the two switches would silently
# cancel each other out when combined.
DRIFT_DROP_COLUMN: dict[str, str] = {
    "gl_entry": "currency",
    "gl_adjustment": "currency",
    "dim_account_src": "account_type",
    "dim_cost_center_src": "name",
    "fx_rate": "rate_to_base",
}


def format_date(value: date) -> str:
    return value.strftime(DATE_FORMAT)


def parse_date(value: str) -> date:
    """Accepts a date or a timestamp; `posted_at` carries the time of day."""
    return datetime.strptime(value[:10], DATE_FORMAT).date()


def format_amount(value: Decimal) -> str:
    """Fixed two places, always from Decimal. float repr is not stable enough to
    compare bytes against."""
    return str(value.quantize(AMOUNT_PLACES))


def format_rate(value: Decimal) -> str:
    """Fixed six places, always from Decimal.

    Deliberately a separate function rather than a `places` argument on
    `format_amount`: the two are different quantities with different rules, and a
    shared formatter with a parameter is how the confusion got in - the rate path
    reached for the amount formatter because it was there.
    """
    return str(value.quantize(RATE_PLACES))


def period_of(value: date) -> str:
    return value.strftime("%Y-%m")


def period_close(accounting_date: str | date) -> date:
    """The last day of the accounting period an entry belongs to.

    Lateness is measured from here rather than from the entry's own date: an entry
    posted three days after it was booked is ordinary, one posted a month after its
    period closed is not.
    """
    value = parse_date(accounting_date) if isinstance(accounting_date, str) else accounting_date
    first_of_next = (value.replace(day=1) + timedelta(days=32)).replace(day=1)
    return first_of_next - timedelta(days=1)
