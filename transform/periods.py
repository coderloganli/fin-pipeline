"""Accounting-period arithmetic, and the windows the mart's comparison columns use.

Pure Python, no database and no engine. It lives here rather than beside the model that
reads it because three callers need the same answer: the mart build, which computes the
comparison columns; the backfill, which has to know which periods a change reaches; and
`ml/`, which judges the periods a run touched.

The window constants are declared once, here, because `dirty_closure` reads them. A
window written down twice is one that stops being backfilled the moment it widens.
See docs/adr/0040.
"""

__all__ = ["MOM_LAG", "YOY_LAG", "ROLLING_SPAN",
           "parse_periods", "shift", "dirty_closure", "period_range"]

# The windows the comparison columns are computed over.
MOM_LAG = 1
YOY_LAG = 12
ROLLING_SPAN = 3


def parse_periods(text: str) -> tuple[str, str]:
    """`2026-01:2026-12` into its ends."""
    first, _, last = text.partition(":")
    if not last or first > last:
        raise ValueError(f"--periods must be FROM:TO with FROM <= TO, got {text!r}")
    return first, last


def shift(period: str, months: int) -> str:
    """`2026-11` and 3 gives `2027-02`."""
    year, month = (int(part) for part in period.split("-"))
    index = (year * 12 + month - 1) + months
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def dirty_closure(periods, *, last_period: str) -> set[str]:
    """The periods a set of dirty periods drags with it.

    Forwards only, and read off the window constants rather than restated: a dirty
    period M changes M's own balance, M+MOM_LAG's month-on-month comparison, the
    rolling mean of every period the span still reaches, and M+YOY_LAG's year-on-year
    one. Widening a window therefore widens this, which is the property that stops a
    backfill quietly skipping periods the aggregate had changed.

    Nothing reaches backwards. No column of M-1's row reads forward. See docs/adr/0040.
    """
    closed: set[str] = set()
    for period in periods:
        candidates = [period, shift(period, MOM_LAG), shift(period, YOY_LAG)]
        candidates += [shift(period, step) for step in range(1, ROLLING_SPAN)]
        closed.update(p for p in candidates if p <= last_period)
    return closed


def period_range(first: str, last: str) -> list[str]:
    """Every period from `first` to `last` inclusive."""
    found, period = [], first
    while period <= last:
        found.append(period)
        period = shift(period, 1)
    return found
