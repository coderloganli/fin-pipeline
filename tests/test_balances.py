"""Monthly balances: signed by the account's normal side, on a dense grid.

Two properties this module exists to establish. Every account's ordinary activity reads
as a positive number that grows, so neither the report nor the anomaly model has to
know which way an account points - and the sign comes from the account type that was in
force in the period, not today's. And every active combination carries a row for every
period in range, because a period-over-period comparison over a sparse table silently
becomes a comparison with the last month that had activity.

Zero and null are different facts. Zero is a measurement; null is the absence of
anything to compare against. NaN is neither and is never produced.

See docs/adr/0032 and 0033. Cases 20-28 of task.md.
"""

from decimal import Decimal

import pytest

from ingest import contracts, raw
from transform.spark import balances, facts, scd2

DIM_CC = contracts.load("dim_cost_center_src")
DIM_ACCOUNT = contracts.load("dim_account_src")
DIM_FX = contracts.load("fx_rate")
GL_ENTRY = contracts.load("gl_entry")

RUN_A = "20260901T031500Z-aaaaaa"

CENTRES = [{"cc_code": "CC-001", "name": "Ops", "dept_code": "DEPT-OPS",
            "effective_date": "2020-01-01"}]
RATES = [{"currency": "CNY", "rate_date": "2026-01-01", "rate_to_base": "1.000000"}]

EXPENSE = {"account_code": "660204", "name": "Office supplies", "parent_code": "6602",
           "account_type": "expense", "effective_date": "2020-01-01"}
REVENUE = {"account_code": "600101", "name": "Product sales", "parent_code": "6001",
           "account_type": "revenue", "effective_date": "2020-01-01"}


def entry(entry_id, accounting_date, account, dr="0.00", cr="0.00"):
    return {
        "entry_id": entry_id, "version": "1",
        "accounting_date": accounting_date, "posted_at": accounting_date,
        "account_code": account, "cost_center_code": "CC-001",
        "currency": "CNY", "amount_dr": dr, "amount_cr": cr,
        "doc_id": f"DOC-{entry_id}", "vendor_code": "", "description": "x",
    }


@pytest.fixture
def aggregated(tmp_path, spark):
    def build(entries, accounts=(EXPENSE, REVENUE), periods=None):
        raw_dir, staging = tmp_path / "raw", tmp_path / "staging"
        raw.merge_table(GL_ENTRY, raw_dir, entries, run_id=RUN_A)
        raw.merge_table(DIM_ACCOUNT, raw_dir, list(accounts), run_id=RUN_A)
        raw.merge_table(DIM_CC, raw_dir, CENTRES, run_id=RUN_A)
        raw.merge_table(DIM_FX, raw_dir, RATES, run_id=RUN_A)
        for contract in (DIM_ACCOUNT, DIM_CC, DIM_FX):
            scd2.build(spark, contract, raw_dir, staging)
        facts.build(spark, raw_dir, staging)
        balances.build(spark, staging, periods=periods)
        return staging
    return build


def series(spark, staging, account):
    return {
        row["accounting_period"]: row
        for row in balances.read(spark, staging)
        if row["account_code"] == account
    }


# --- the sign ---------------------------------------------------------------

def test_an_expense_with_more_debits_reads_positive(spark, aggregated):
    """Case 20. Debit-normal: expenses grow on the debit side."""
    staging = aggregated([entry("E1", "2026-01-15", "660204", dr="500.00"),
                          entry("E2", "2026-01-20", "660204", cr="100.00")])
    january = series(spark, staging, "660204")["2026-01"]

    assert january["balance_as_reported"] == Decimal("400.00")
    assert january["debit_total"] == Decimal("500.00")
    assert january["credit_total"] == Decimal("100.00")


def test_revenue_with_more_credits_also_reads_positive(spark, aggregated):
    """Case 21. Credit-normal. Under a plain debits-minus-credits this would be -400,
    and revenue growing would read as the number going further below zero."""
    staging = aggregated([entry("R1", "2026-01-15", "600101", cr="500.00"),
                          entry("R2", "2026-01-20", "600101", dr="100.00")])
    assert series(spark, staging, "600101")["2026-01"]["balance_as_reported"] == Decimal("400.00")


def test_the_sign_uses_the_account_type_the_period_had(spark, aggregated):
    """Case 27. An account reclassified from revenue to expense in April does not
    retroactively flip the sign of periods that closed before the change - the type
    comes from the point-in-time attribution, so this falls out rather than being
    arranged."""
    reclassified = [
        {**REVENUE, "account_code": "600101", "effective_date": "2020-01-01"},
        {**REVENUE, "account_code": "600101", "account_type": "expense",
         "parent_code": "6602", "effective_date": "2026-04-01"},
    ]
    staging = aggregated(
        [entry("A", "2026-03-15", "600101", cr="500.00"),
         entry("B", "2026-05-15", "600101", cr="500.00")],
        accounts=(EXPENSE, *reclassified),
    )
    got = series(spark, staging, "600101")
    assert got["2026-03"]["balance_as_reported"] == Decimal("500.00")    # credit-normal in March
    assert got["2026-05"]["balance_as_reported"] == Decimal("-500.00")   # debit-normal in May


# --- agreement with the detail ----------------------------------------------

def test_the_totals_agree_with_the_entries_added_up(spark, aggregated):
    """Case 22. An analyst who filters the fact table to one account and one month and
    totals the column has to get the number the report shows - which is only true
    because the aggregate sums figures already rounded at the line. See docs/adr/0031."""
    entries = [entry(f"E{n}", "2026-01-15", "660204", dr="333.33") for n in range(3)]
    staging = aggregated(entries)

    landed = [row for row in facts.read(spark, staging) if row["account_code"] == "660204"]
    by_hand = sum(row["amount_dr_base"] for row in landed)
    assert series(spark, staging, "660204")["2026-01"]["debit_total"] == by_hand


# --- the dense grid ---------------------------------------------------------

def test_a_period_with_no_entries_still_carries_a_row(spark, aggregated):
    """Case 23. Without it, "compared with last month" silently becomes "compared with
    the last month that had activity"."""
    staging = aggregated([entry("A", "2026-01-15", "660204", dr="100.00"),
                          entry("B", "2026-03-15", "660204", dr="300.00")])
    got = series(spark, staging, "660204")

    assert "2026-02" in got
    assert got["2026-02"]["balance_as_reported"] == Decimal("0.00")
    assert got["2026-02"]["debit_total"] == Decimal("0.00")


def test_an_explicit_range_wider_than_the_facts_is_honoured(spark, aggregated):
    """Case 23a. This is what a grid derived from min/max of the entry dates would
    fail. It passes case 23 - the interior gap is filled - and silently drops a leading
    or trailing period in which nothing was posted anywhere."""
    staging = aggregated([entry("A", "2026-03-15", "660204", dr="100.00")],
                         periods="2026-01:2026-05")
    got = series(spark, staging, "660204")

    assert sorted(got) == ["2026-01", "2026-02", "2026-03", "2026-04", "2026-05"]
    assert got["2026-01"]["balance_as_reported"] == Decimal("0.00")
    assert got["2026-05"]["balance_as_reported"] == Decimal("0.00")


def test_without_a_range_the_fact_span_is_used_and_named(spark, aggregated, capsys):
    """Case 23b. The fallback exists so the job is usable by hand, and it announces
    itself so nobody mistakes it for the range they asked for."""
    staging = aggregated([entry("A", "2026-02-15", "660204", dr="100.00"),
                          entry("B", "2026-04-15", "660204", dr="100.00")])
    assert sorted(series(spark, staging, "660204")) == ["2026-02", "2026-03", "2026-04"]
    assert "2026-02" in capsys.readouterr().out


# --- the comparisons --------------------------------------------------------

def test_the_first_period_has_no_comparison_at_all(spark, aggregated):
    """Case 24. Null, not zero: zero would teach a model a jump that never happened."""
    staging = aggregated([entry("A", "2026-01-15", "660204", dr="100.00"),
                          entry("B", "2026-02-15", "660204", dr="150.00")])
    first = series(spark, staging, "660204")["2026-01"]

    assert first["balance_delta_mom"] is None
    assert first["balance_pct_mom"] is None
    assert first["balance_delta_yoy"] is None


def test_a_zero_base_gives_a_delta_but_no_percentage(spark, aggregated):
    """Case 25. This is where the two columns part company, and it is the case an
    anomaly investigation cares about most: an account that had nothing and now has
    something. A ratio against zero is undefined; the difference is not."""
    staging = aggregated([entry("A", "2026-02-15", "660204", dr="500.00")],
                         periods="2026-01:2026-02")
    february = series(spark, staging, "660204")["2026-02"]

    assert february["balance_delta_mom"] == Decimal("500.00")
    assert february["balance_pct_mom"] is None


def test_falling_to_zero_is_minus_one_hundred_per_cent(spark, aggregated):
    """Case 25a. The other direction, where the percentage is defined."""
    staging = aggregated([entry("A", "2026-01-15", "660204", dr="500.00")],
                         periods="2026-01:2026-02")
    february = series(spark, staging, "660204")["2026-02"]

    assert february["balance_delta_mom"] == Decimal("-500.00")
    assert february["balance_pct_mom"] == Decimal("-1.000000")


def test_a_negative_balance_growing_more_negative_reads_as_a_fall(spark, aggregated):
    """Case 25b. The denominator is an absolute value. Without that, -100 to -200
    divides by a negative and reports as growth."""
    staging = aggregated([entry("A", "2026-01-15", "660204", cr="100.00"),
                          entry("B", "2026-02-15", "660204", cr="200.00")],
                         periods="2026-01:2026-02")
    february = series(spark, staging, "660204")["2026-02"]

    assert february["balance_as_reported"] == Decimal("-200.00")
    assert february["balance_pct_mom"] == Decimal("-1.000000")


def test_no_column_holds_a_nan(spark, aggregated):
    """Case 25c. NaN propagates through arithmetic silently and compares false with
    itself, so a filter written to exclude it and one written to find it can both come
    back empty."""
    staging = aggregated([entry("A", "2026-01-15", "660204", dr="100.00"),
                          entry("B", "2026-03-15", "660204", dr="0.00")])
    for row in balances.read(spark, staging):
        for name, value in row.items():
            assert value == value, f"{name} holds NaN"


def test_the_rolling_mean_says_how_many_periods_it_used(spark, aggregated):
    """Case 26. A three-month mean computed from one month is a number, and without the
    count beside it there is no telling it from one that had three."""
    staging = aggregated([entry(f"E{n}", f"2026-0{n}-15", "660204", dr="300.00")
                          for n in (1, 2, 3)])
    got = series(spark, staging, "660204")

    assert got["2026-01"]["rolling_periods"] == 1
    assert got["2026-03"]["rolling_periods"] == 3
    assert got["2026-03"]["balance_rolling_3m"] == Decimal("300.00")


def test_three_runs_over_one_fact_table_agree(spark, aggregated):
    """Case 28."""
    staging = aggregated([entry("A", "2026-01-15", "660204", dr="100.00")])
    seen = []
    for _ in range(3):
        balances.build(spark, staging)
        rows = balances.read(spark, staging)
        seen.append((len(rows), balances.checksum(rows)))
    assert seen[0] == seen[1] == seen[2], seen


# --- added at stage 8 -------------------------------------------------------

SECOND_CENTRE = {"cc_code": "CC-002", "name": "Sales", "dept_code": "DEPT-SALES",
                 "effective_date": "2020-01-01"}


def test_every_combination_carries_every_period_and_the_right_sign(spark, aggregated,
                                                                   monkeypatch):
    """Review finding 5. The sign and density cases each picked one account out of a
    fixture with one cost centre, so a defect affecting another combination, the credit
    total, or the other account type would pass. This asserts across every combination
    in the output."""
    monkeypatch.setattr(balances, "CENTRES", None, raising=False)
    entries = [
        entry("A", "2026-01-15", "660204", dr="500.00"),
        entry("B", "2026-03-15", "600101", cr="700.00"),
    ]
    staging = aggregated(entries, periods="2026-01:2026-03")

    rows = balances.read(spark, staging)
    combinations = {(r["account_code"], r["cost_center_code"]) for r in rows}
    assert combinations == {("660204", "CC-001"), ("600101", "CC-001")}

    for combination in combinations:
        periods = sorted(r["accounting_period"] for r in rows
                         if (r["account_code"], r["cost_center_code"]) == combination)
        assert periods == ["2026-01", "2026-02", "2026-03"], combination

    for row in rows:
        debit_normal = row["account_type"] in balances.DEBIT_NORMAL
        expected = (row["debit_total"] - row["credit_total"]) if debit_normal else (
            row["credit_total"] - row["debit_total"])
        assert row["balance_as_reported"] == expected, row


def test_both_totals_agree_with_the_detail_for_every_combination(spark, aggregated):
    """Review finding 5 again: case 22 checked the debit total of one account."""
    entries = [
        entry("A", "2026-01-15", "660204", dr="333.33"),
        entry("B", "2026-01-16", "660204", cr="111.11"),
        entry("C", "2026-01-17", "600101", cr="777.77"),
        entry("D", "2026-01-18", "600101", dr="222.22"),
    ]
    staging = aggregated(entries, periods="2026-01:2026-01")

    landed = facts.read(spark, staging)
    for row in balances.read(spark, staging):
        detail = [f for f in landed
                  if f["account_code"] == row["account_code"]
                  and f["cost_center_code"] == row["cost_center_code"]]
        assert row["debit_total"] == sum(f["amount_dr_base"] for f in detail)
        assert row["credit_total"] == sum(f["amount_cr_base"] for f in detail)


def test_a_combination_active_only_in_the_last_period_is_typed(spark, aggregated):
    """Review finding 7. The carry-forward has a backward half for the periods before
    a combination's first activity, and every density case so far had activity on both
    sides of its gap - so only the forward half was ever exercised."""
    staging = aggregated([entry("A", "2026-03-15", "660204", dr="500.00")],
                         periods="2026-01:2026-03")
    got = series(spark, staging, "660204")

    assert sorted(got) == ["2026-01", "2026-02", "2026-03"]
    for period in got.values():
        assert period["account_type"] == "expense"
        assert period["balance_as_reported"] is not None


def test_a_year_over_year_comparison_appears_at_month_thirteen(spark, aggregated):
    """Review finding 8. `lag(12)` was only ever inspected on the first row, where it
    is null for the same reason the month-over-month is."""
    entries = [entry("A", "2026-02-15", "660204", dr="100.00"),
               entry("B", "2027-02-15", "660204", dr="150.00")]
    staging = aggregated(entries, periods="2026-01:2027-02")
    got = series(spark, staging, "660204")

    assert got["2026-12"]["balance_delta_yoy"] is None, "before a year has passed"
    assert got["2027-02"]["balance_delta_yoy"] == Decimal("50.00")
    assert got["2027-01"]["balance_delta_yoy"] == Decimal("0.00")


def test_a_short_range_has_no_year_over_year_anywhere(spark, aggregated):
    """The other half of finding 8."""
    staging = aggregated([entry("A", "2026-02-15", "660204", dr="100.00")],
                         periods="2026-01:2026-06")
    for row in balances.read(spark, staging):
        assert row["balance_delta_yoy"] is None, row["accounting_period"]
        assert row["balance_pct_yoy"] is None, row["accounting_period"]


def test_the_rolling_mean_rounds_to_cents(spark, aggregated):
    """Review finding 9. The mean of 100, 100 and 101 is 100.333..., and the column is
    cents like every other money column here. Asserted rather than left to whatever
    `avg` happened to produce."""
    entries = [entry("A", "2026-01-15", "660204", dr="100.00"),
               entry("B", "2026-02-15", "660204", dr="100.00"),
               entry("C", "2026-03-15", "660204", dr="101.00")]
    staging = aggregated(entries, periods="2026-01:2026-03")
    march = series(spark, staging, "660204")["2026-03"]

    assert march["rolling_periods"] == 3
    assert march["balance_rolling_3m"] == Decimal("100.33")


# --- as-reported and as-restated (ADR 0043) ---------------------------------
#
# Cases 27-33 of task.md. A correction amends the period's figure; a restatement keeps
# the original basis and presents a new one alongside it. Three columns:
# `balance_as_reported` (entries and corrections), `restatement_delta`, and
# `balance_as_restated`, their sum.

GL_ADJUSTMENT = contracts.load("gl_adjustment")


def adjustment(entry_id, accounting_date, account, kind, dr="0.00", cr="0.00"):
    return {
        "entry_id": entry_id, "version": "2",
        "accounting_date": accounting_date, "posted_at": accounting_date,
        "account_code": account, "cost_center_code": "CC-001",
        "currency": "CNY", "amount_dr": dr, "amount_cr": cr,
        "doc_id": entry_id, "adjusts_entry_id": "E1", "adjustment_type": kind,
        "vendor_code": "", "description": "x",
    }


@pytest.fixture
def adjusted(tmp_path, spark):
    """The aggregate with an adjustment fact beside the entry fact."""

    def build(entries, adjustments=(), accounts=(EXPENSE, REVENUE), periods=None):
        raw_dir, staging = tmp_path / "raw", tmp_path / "staging"
        raw.merge_table(GL_ENTRY, raw_dir, entries, run_id=RUN_A)
        raw.merge_table(GL_ADJUSTMENT, raw_dir, list(adjustments), run_id=RUN_A)
        raw.merge_table(DIM_ACCOUNT, raw_dir, list(accounts), run_id=RUN_A)
        raw.merge_table(DIM_CC, raw_dir, CENTRES, run_id=RUN_A)
        raw.merge_table(DIM_FX, raw_dir, RATES, run_id=RUN_A)
        for contract in (DIM_ACCOUNT, DIM_CC, DIM_FX):
            scd2.build(spark, contract, raw_dir, staging)
        facts.build(spark, raw_dir, staging)
        facts.build(spark, raw_dir, staging, model=facts.ADJUSTMENT_MODEL)
        balances.build(spark, staging, periods=periods)
        return raw_dir, staging

    return build


def test_with_no_adjustments_the_two_bases_agree(spark, adjusted):
    """Case 27. The pair is meaningful because the two coincide until something makes
    them differ, not because they are always two different numbers."""
    _, staging = adjusted([entry("E1", "2026-03-15", "660204", dr="100.00")],
                          periods="2026-01:2026-06")

    for row in balances.read(spark, staging):
        assert row["restatement_delta"] == Decimal("0.00"), row["accounting_period"]
        assert row["balance_as_reported"] == row["balance_as_restated"]


def test_a_correction_moves_both_bases(spark, adjusted):
    """Case 28. A correction amends the period's number: after it lands there is one
    figure and it is the amended one."""
    _, staging = adjusted(
        [entry("E1", "2026-03-15", "660204", dr="100.00")],
        adjustments=[adjustment("A-C", "2026-03-20", "660204", "correction", dr="25.00")],
        periods="2026-01:2026-06",
    )
    march = series(spark, staging, "660204")["2026-03"]

    assert march["debit_total"] == Decimal("125.00")
    assert march["balance_as_reported"] == Decimal("125.00")
    assert march["restatement_delta"] == Decimal("0.00")
    assert march["balance_as_restated"] == Decimal("125.00")


def test_a_restatement_moves_only_the_restated_basis(spark, adjusted):
    """Case 29. The original basis is preserved and the new one given alongside it,
    which is the distinction docs/product.md puts at the centre of this platform."""
    _, staging = adjusted(
        [entry("E1", "2026-03-15", "660204", dr="100.00")],
        adjustments=[adjustment("A-R", "2026-03-20", "660204", "restatement", dr="25.00")],
        periods="2026-01:2026-06",
    )
    march = series(spark, staging, "660204")["2026-03"]

    assert march["debit_total"] == Decimal("100.00")
    assert march["balance_as_reported"] == Decimal("100.00")
    assert march["restatement_delta"] == Decimal("25.00")
    assert march["balance_as_restated"] == Decimal("125.00")


def test_the_restatement_delta_is_signed_by_the_normal_side(spark, adjusted):
    """Case 30. The delta is a balance movement, so it is signed the way docs/adr/0032
    signs every balance - not left as a raw debit-minus-credit."""
    _, credit_side = adjusted(
        [entry("R1", "2026-03-15", "600101", cr="100.00")],
        adjustments=[adjustment("A-R", "2026-03-20", "600101", "restatement", cr="25.00")],
        periods="2026-01:2026-06",
    )
    assert series(spark, credit_side, "600101")["2026-03"]["restatement_delta"] == \
        Decimal("25.00")

    _, debit_side = adjusted(
        [entry("E1", "2026-03-15", "660204", dr="100.00")],
        adjustments=[adjustment("A-R", "2026-03-20", "660204", "restatement", cr="25.00")],
        periods="2026-01:2026-06",
    )
    assert series(spark, debit_side, "660204")["2026-03"]["restatement_delta"] == \
        Decimal("-25.00")


def test_the_reported_basis_reconciles_from_the_totals(spark, adjusted):
    """Case 31. The row's arithmetic stays checkable by hand, which is why the debit
    and credit totals stay on the as-reported basis."""
    _, staging = adjusted(
        [entry("E1", "2026-03-15", "660204", dr="100.00"),
         entry("E2", "2026-03-16", "660204", cr="30.00"),
         entry("R1", "2026-03-15", "600101", cr="500.00")],
        adjustments=[adjustment("A-C", "2026-03-20", "660204", "correction", dr="25.00"),
                     adjustment("A-R", "2026-03-21", "660204", "restatement", dr="7.00")],
        periods="2026-01:2026-06",
    )

    for row in balances.read(spark, staging):
        difference = row["debit_total"] - row["credit_total"]
        expected = difference if row["account_type"] == "expense" else -difference
        assert row["balance_as_reported"] == expected, row
        assert row["balance_as_restated"] == \
            row["balance_as_reported"] + row["restatement_delta"]


def test_the_windowed_columns_read_the_restated_basis(spark, adjusted):
    """Case 32. The anomaly model and the report should judge the best current answer;
    a delta computed on a superseded basis flags movements that are artefacts of not
    having looked at the restatement."""
    _, staging = adjusted(
        [entry("E1", "2026-03-15", "660204", dr="100.00"),
         entry("E2", "2026-04-15", "660204", dr="100.00")],
        adjustments=[adjustment("A-R", "2026-03-20", "660204", "restatement", dr="25.00")],
        periods="2026-01:2026-06",
    )
    months = series(spark, staging, "660204")

    assert months["2026-03"]["balance_as_reported"] == \
        months["2026-04"]["balance_as_reported"]
    assert months["2026-04"]["balance_delta_mom"] == Decimal("-25.00")


def test_the_old_balance_column_is_gone(spark, adjusted):
    """Case 33. It meant "entries only", which after this change is none of the three
    columns above, and a column whose name stopped matching its contents is how a wrong
    number reaches a report."""
    _, staging = adjusted([entry("E1", "2026-03-15", "660204", dr="100.00")],
                          periods="2026-01:2026-06")

    columns = set(balances.frame(spark, staging).columns)
    assert "balance" not in columns
    assert {"balance_as_reported", "restatement_delta", "balance_as_restated"} <= columns


# --- selective recomputation (ADR 0041) -------------------------------------
#
# Cases 34-37 of task.md. Writes are scoped to the dirty closure; reads are not, because
# the dense grid's membership and the type carried into an empty period are properties
# of the whole fact table.

WIDE = "2026-01:2027-12"


def aggregate_mtimes(staging) -> dict[str, int]:
    root = balances.staging_path(staging)
    return {
        directory.name.split("=", 1)[1]: max(
            path.stat().st_mtime_ns for path in directory.iterdir() if path.is_file()
        )
        for directory in sorted(root.glob("accounting_period=*"))
    }


def test_a_dirty_build_writes_the_closure_and_nothing_else(spark, adjusted):
    """Case 34. The ticket's acceptance criterion at the aggregate: March drags April,
    May and next March, and every other partition's file is untouched."""
    _, staging = adjusted(
        [entry("E1", "2026-01-15", "660204", dr="100.00"),
         entry("E2", "2026-03-15", "660204", dr="100.00"),
         entry("E3", "2026-08-15", "660204", dr="100.00")],
        periods=WIDE,
    )
    before = aggregate_mtimes(staging)

    balances.build(spark, staging, periods=WIDE, dirty={"2026-03"})
    after = aggregate_mtimes(staging)

    closure = {"2026-03", "2026-04", "2026-05", "2027-03"}
    for period in before:
        if period in closure:
            assert after[period] != before[period], period
        else:
            assert after[period] == before[period], period


def test_the_dense_grid_survives_a_selective_build(spark, adjusted):
    """Case 35. A combination that posts only in January still carries a zero row in a
    selectively rebuilt March, with the type it carries in January."""
    _, staging = adjusted(
        [entry("E1", "2026-01-15", "660204", dr="100.00"),
         entry("E2", "2026-03-15", "600101", cr="100.00")],
        periods=WIDE,
    )

    balances.build(spark, staging, periods=WIDE, dirty={"2026-03"})
    march = series(spark, staging, "660204")["2026-03"]

    assert march["balance_as_reported"] == Decimal("0.00")
    assert march["account_type"] == "expense"


def test_a_new_combination_forces_a_full_rewrite(spark, adjusted, tmp_path):
    """Case 35a. Density is a statement about the whole table, so a combination seen
    for the first time needs a zero row in every period - not only in the closure."""
    raw_dir, staging = adjusted(
        [entry("E1", "2026-01-15", "660204", dr="100.00")], periods=WIDE,
    )
    before = aggregate_mtimes(staging)

    fresh = {**entry("E-NEW", "2026-03-15", "600101", cr="50.00"),
             "cost_center_code": "CC-001"}
    raw.merge_table(GL_ENTRY, raw_dir, [fresh], run_id=RUN_A)
    facts.build(spark, raw_dir, staging, dirty={"2026-03"})
    balances.build(spark, staging, periods=WIDE, dirty={"2026-03"})

    after = aggregate_mtimes(staging)
    for period in before:
        assert after[period] != before[period], period

    landed = series(spark, staging, "600101")
    assert set(landed) == set(before)


def test_a_combination_that_lost_its_last_entry_forces_a_full_rewrite(spark, adjusted):
    """Case 35b. The same failure with the sign reversed: a stale row in every period,
    which no gate catches because it is a row that is present and well formed."""
    raw_dir, staging = adjusted(
        [entry("E1", "2026-01-15", "660204", dr="100.00"),
         entry("E2", "2026-03-15", "600101", cr="100.00")],
        periods=WIDE,
    )
    before = aggregate_mtimes(staging)

    for path in raw.partitions(GL_ENTRY, raw_dir):
        if path.parent.name == "accounting_period=2026-03":
            path.unlink()
    facts.build(spark, raw_dir, staging, dirty={"2026-03"})
    balances.build(spark, staging, periods=WIDE, dirty={"2026-03"})

    after = aggregate_mtimes(staging)
    for period in before:
        assert after[period] != before[period], period
    assert series(spark, staging, "600101") == {}


def test_an_ordinary_late_entry_does_not_force_a_full_rewrite(spark, adjusted):
    """Case 35c. The fallback fires on a membership change, not on a late entry - which
    is what keeps it an exception rather than the rule."""
    raw_dir, staging = adjusted(
        [entry("E1", "2026-01-15", "660204", dr="100.00"),
         entry("E2", "2026-03-15", "660204", dr="100.00")],
        periods=WIDE,
    )
    before = aggregate_mtimes(staging)

    late = entry("E-LATE", "2026-03-20", "660204", dr="10.00")
    raw.merge_table(GL_ENTRY, raw_dir, [late], run_id=RUN_A)
    facts.build(spark, raw_dir, staging, dirty={"2026-03"})
    balances.build(spark, staging, periods=WIDE, dirty={"2026-03"})

    after = aggregate_mtimes(staging)
    assert after["2026-01"] == before["2026-01"]
    assert after["2026-08"] == before["2026-08"]


def test_the_account_type_of_a_zero_row_survives_a_selective_build(spark, adjusted,
                                                                   tmp_path):
    """Case 36. The type fill reaches backwards and forwards over every period the
    combination has, so a build that read only the dirty periods would answer
    differently."""
    entries = [entry("E1", "2026-06-15", "660204", dr="100.00")]
    _, staging = adjusted(entries, periods=WIDE)
    balances.build(spark, staging, periods=WIDE, dirty={"2026-03"})
    selective = series(spark, staging, "660204")["2026-03"]

    balances.build(spark, staging, periods=WIDE)
    full = series(spark, staging, "660204")["2026-03"]

    assert selective["account_type"] == full["account_type"] == "expense"


def test_a_selective_build_equals_a_full_build(spark, adjusted):
    """Case 37. Column for column, over the whole table. This is the property that
    makes selective recomputation an optimisation rather than a second answer."""
    raw_dir, staging = adjusted(
        [entry("E1", "2026-01-15", "660204", dr="100.00"),
         entry("E2", "2026-03-15", "660204", dr="100.00"),
         entry("E3", "2026-08-15", "600101", cr="100.00")],
        periods=WIDE,
    )
    late = entry("E-LATE", "2026-03-20", "660204", dr="10.00")
    raw.merge_table(GL_ENTRY, raw_dir, [late], run_id=RUN_A)
    facts.build(spark, raw_dir, staging, dirty={"2026-03"})

    balances.build(spark, staging, periods=WIDE, dirty={"2026-03"})
    selective = balances.checksum(balances.read(spark, staging))

    balances.build(spark, staging, periods=WIDE)
    full = balances.checksum(balances.read(spark, staging))

    assert selective == full
