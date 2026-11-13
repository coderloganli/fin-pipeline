"""The window closure: which periods a change reaches.

`transform/periods.py` is the one place the comparison windows are written down, and
`dirty_closure` reads them rather than restating them. That is the property these cases
pin: widening a window widens the closure, so a backfill cannot quietly skip a period
whose figures the change had altered.

The closure matters to two callers. `judge` uses it to decide which periods a daily run
has to re-judge, and a person reading `--periods` expects the range they asked for to be
the range that gets rebuilt. See docs/adr/0040.
"""

from transform import periods


def test_a_dirty_period_drags_its_windows_with_it():
    """Month-on-month reaches M+1, the three-month rolling mean M+2, and year-on-year
    M+12."""
    assert periods.dirty_closure({"2026-03"}, last_period="2027-12") == {
        "2026-03", "2026-04", "2026-05", "2027-03",
    }


def test_the_closure_is_clipped_to_the_reporting_range():
    """A period outside the range has no row in the mart to recompute."""
    assert periods.dirty_closure({"2026-03"}, last_period="2026-06") == {
        "2026-03", "2026-04", "2026-05",
    }


def test_the_closure_is_derived_from_the_window_constants(monkeypatch):
    """Widening the rolling mean widens the closure, because the closure reads the
    constant rather than restating it. A window written down twice is one that stops
    being backfilled the moment it widens."""
    monkeypatch.setattr(periods, "ROLLING_SPAN", 6)
    assert "2026-08" in periods.dirty_closure({"2026-03"}, last_period="2027-12")


def test_the_closure_of_nothing_is_nothing_and_it_does_not_duplicate():
    assert periods.dirty_closure(set(), last_period="2026-12") == set()
    assert periods.dirty_closure({"2026-03", "2026-04"}, last_period="2026-12") == {
        "2026-03", "2026-04", "2026-05", "2026-06",
    }


def test_a_range_must_run_forwards():
    """`--periods` is FROM:TO. Reversed, it would silently aggregate nothing."""
    import pytest

    with pytest.raises(ValueError, match="FROM <= TO"):
        periods.parse_periods("2026-12:2026-01")


def test_a_range_lists_every_period_inclusive():
    assert periods.period_range("2026-11", "2027-02") == [
        "2026-11", "2026-12", "2027-01", "2027-02",
    ]
