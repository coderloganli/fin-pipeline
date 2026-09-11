"""Attributing each entry to the structure that was in force on its accounting date.

The property this module exists to establish: re-running a closed period returns what
it returned at the time. A cost centre that moves department in July does not change
what March reported, and an account reclassified in April does not change what March
reported either.

Every attribution is one join shape - the natural key, *and* the date inside the
version's interval. Both halves: with the range alone every fact matches every version
valid on its date, which does not raise, it multiplies. See docs/adr/0029.

Cases 11-19 of task.md.
"""

from decimal import Decimal

import pytest

from ingest import contracts, raw
from transform.spark import facts, scd2

DIM_CC = contracts.load("dim_cost_center_src")
DIM_ACCOUNT = contracts.load("dim_account_src")
DIM_FX = contracts.load("fx_rate")
GL_ENTRY = contracts.load("gl_entry")

RUN_A = "20260901T031500Z-aaaaaa"

# A cost centre that moves department in July, and an account reclassified in April.
COST_CENTRES = [
    {"cc_code": "CC-002", "name": "Sales - North", "dept_code": "DEPT-RND",
     "effective_date": "2020-01-01"},
    {"cc_code": "CC-002", "name": "Sales - North", "dept_code": "DEPT-OPS",
     "effective_date": "2026-07-01"},
]
ACCOUNTS = [
    {"account_code": "660204", "name": "Office supplies", "parent_code": "6602",
     "account_type": "expense", "effective_date": "2020-01-01"},
    {"account_code": "660204", "name": "Office supplies", "parent_code": "6601",
     "account_type": "expense", "effective_date": "2026-04-01"},
]
# 2026-03-13 is a Friday; the next published rate is Monday the 16th.
RATES = [
    {"currency": "CNY", "rate_date": "2026-01-01", "rate_to_base": "1.000000"},
    {"currency": "EUR", "rate_date": "2026-01-01", "rate_to_base": "7.000000"},
    {"currency": "EUR", "rate_date": "2026-03-13", "rate_to_base": "7.654321"},
    {"currency": "EUR", "rate_date": "2026-03-16", "rate_to_base": "7.800000"},
    {"currency": "EUR", "rate_date": "2026-07-01", "rate_to_base": "8.000000"},
]


def entry(entry_id, accounting_date, **overrides):
    row = {
        "entry_id": entry_id, "version": "1",
        "accounting_date": accounting_date, "posted_at": accounting_date,
        "account_code": "660204", "cost_center_code": "CC-002",
        "currency": "EUR", "amount_dr": "1234.56", "amount_cr": "0.00",
        "doc_id": f"DOC-{entry_id}", "vendor_code": "V-0001",
        "description": "Office supplies",
    }
    row.update(overrides)
    return row


ENTRIES = [
    entry("E-MAR", "2026-03-16"),          # a published rate day
    entry("E-SAT", "2026-03-14"),          # a Saturday: Friday's rate applies
    entry("E-JUL", "2026-07-15"),          # after both changes
]


@pytest.fixture
def built(tmp_path, spark):
    """Raw loaded, the three dimensions chained, and the fact table built."""
    def build(entries=ENTRIES, accounts=ACCOUNTS, centres=COST_CENTRES, rates=RATES):
        raw_dir, staging = tmp_path / "raw", tmp_path / "staging"
        raw.merge_table(GL_ENTRY, raw_dir, entries, run_id=RUN_A)
        raw.merge_table(DIM_ACCOUNT, raw_dir, accounts, run_id=RUN_A)
        raw.merge_table(DIM_CC, raw_dir, centres, run_id=RUN_A)
        raw.merge_table(DIM_FX, raw_dir, rates, run_id=RUN_A)
        for contract in (DIM_ACCOUNT, DIM_CC, DIM_FX):
            scd2.build(spark, contract, raw_dir, staging)
        facts.build(spark, raw_dir, staging)
        return raw_dir, staging
    return build


def by_id(spark, staging):
    return {row["entry_id"]: row for row in facts.read(spark, staging)}


# --- the acceptance scenario ------------------------------------------------

def test_a_march_entry_keeps_the_department_march_had(spark, built):
    """Case 11. The cost centre moved to DEPT-OPS on 1 July. March was reported against
    DEPT-RND at the time and has to stay that way - this is the whole argument."""
    _, staging = built()
    landed = by_id(spark, staging)

    assert landed["E-MAR"]["dept_code"] == "DEPT-RND"
    assert landed["E-JUL"]["dept_code"] == "DEPT-OPS"


def test_a_march_entry_keeps_the_parent_march_had(spark, built):
    """Case 12. The same on the chart: the account was reclassified under 6601 in
    April, so March still rolls up into 6602."""
    _, staging = built()
    landed = by_id(spark, staging)

    assert landed["E-MAR"]["parent_code"] == "6602"
    assert landed["E-SAT"]["parent_code"] == "6602"
    assert landed["E-JUL"]["parent_code"] == "6601"


# --- the rate ---------------------------------------------------------------

def test_a_saturday_entry_takes_the_previous_business_days_rate(spark, built):
    """Case 13. 2026-03-14 is a Saturday and no rate is published for it. Friday's
    interval covers it, so the rate is Friday's - not Monday's, and not null.

    27% of generated entries fall on a weekend, so under an equality join a quarter of
    the ledger would silently lose its base-currency amount."""
    _, staging = built()
    assert by_id(spark, staging)["E-SAT"]["rate_to_base"] == Decimal("7.654321")


def test_an_entry_on_a_published_day_takes_that_days_rate(spark, built):
    """Case 14. The other side of case 13."""
    _, staging = built()
    assert by_id(spark, staging)["E-MAR"]["rate_to_base"] == Decimal("7.800000")


# --- the arithmetic ---------------------------------------------------------

def test_the_base_amount_is_the_product_rounded_at_the_line(spark, built):
    """Case 15. 1234.56 EUR at 7.654321 is 9449.71853376, which rounds to 9449.72. The
    expected value is written out rather than recomputed. See docs/adr/0031."""
    _, staging = built()
    assert by_id(spark, staging)["E-SAT"]["amount_dr_base"] == Decimal("9449.72")


def test_amounts_and_rates_are_decimals_not_doubles(spark, built):
    """Case 16. docs/adr/0013 spent a ticket keeping floats out of rate generation;
    reading them back as doubles would undo it."""
    _, staging = built()
    types = dict(facts.frame(spark, staging).dtypes)
    for column in ("amount_dr", "amount_cr", "amount_dr_base", "amount_cr_base",
                   "rate_to_base"):
        assert types[column].startswith("decimal"), f"{column} is {types[column]}"


def test_the_decimal_precisions_are_the_ones_that_were_measured(spark, built):
    """Case 16a. Exactly, not "large enough". 37 is one short of Spark's cap, and at
    the cap Spark reduces the scale rather than raising - a quiet loss of precision in
    the one calculation docs/adr/0031 exists to protect."""
    _, staging = built()
    types = dict(facts.frame(spark, staging).dtypes)

    assert types["amount_dr"] == f"decimal({facts.AMOUNT_PRECISION},2)"
    assert types["rate_to_base"] == f"decimal({facts.RATE_PRECISION},6)"
    assert types["amount_dr_base"] == f"decimal({facts.BASE_PRECISION},2)"
    assert facts.AMOUNT_PRECISION + facts.RATE_PRECISION + 1 <= 38


def test_rounding_is_half_up_rather_than_half_even(spark, built):
    """Case 16b. `bround` would give 0.12 for the first of these. A ledger rounds
    half-up, and so does whoever checks the arithmetic by hand."""
    ties = [entry("E-TIE-A", "2026-01-05", currency="CNY", amount_dr="0.125"),
            entry("E-TIE-B", "2026-01-05", currency="CNY", amount_dr="0.135")]
    _, staging = built(entries=ties)
    landed = by_id(spark, staging)

    assert landed["E-TIE-A"]["amount_dr_base"] == Decimal("0.13")
    assert landed["E-TIE-B"]["amount_dr_base"] == Decimal("0.14")


# --- the gates --------------------------------------------------------------

def test_a_fact_that_matched_nothing_fails_the_build(spark, built, tmp_path):
    """Case 17. An entry in a currency no rate covers has no base amount, and a fact
    table quietly missing a quarter of its figures is worse than a build that stops."""
    orphan = [entry("E-ORPHAN", "2026-03-16", currency="USD")]
    with pytest.raises(facts.Unattributed) as failure:
        built(entries=ENTRIES + orphan)

    message = str(failure.value)
    assert "E-ORPHAN" in message
    assert "1" in message


def test_three_builds_over_one_raw_layer_agree(spark, built, tmp_path):
    """Case 18."""
    raw_dir, staging = built()
    seen = []
    for _ in range(3):
        facts.build(spark, raw_dir, staging)
        rows = facts.read(spark, staging)
        seen.append((len(rows), facts.checksum(rows)))
    assert seen[0] == seen[1] == seen[2], seen


def test_the_primary_key_survives_the_range_joins(spark, built):
    """Case 19. The join is the natural key *and* the date range. With the range alone
    every entry matches every version valid on its date - which does not raise, it
    multiplies, and the result still adds up to a number somebody might publish."""
    _, staging = built()
    rows = facts.read(spark, staging)
    keys = [(row["entry_id"], row["version"]) for row in rows]

    assert len(keys) == len(set(keys)), f"the join multiplied rows: {keys}"
    assert len(rows) == len(ENTRIES)


def test_the_raw_run_identifiers_are_carried_through(spark, built):
    """Case 19a. `_first_run_id` and `_last_run_id` mean different things
    (docs/adr/0018) and both come across. The transform's own run identity belongs to
    orchestrate-the-daily-run, which already owns reconciling the two sources."""
    _, staging = built()
    row = by_id(spark, staging)["E-MAR"]
    assert row["source_first_run_id"] == RUN_A
    assert row["source_last_run_id"] == RUN_A


# --- added at stage 8 -------------------------------------------------------

def test_an_overlapping_dimension_multiplies_and_is_caught(spark, built, tmp_path):
    """Review finding 10. The orphan check cannot see this: every key is non-null and
    every row looks attributed, the table simply holds more rows than the ledger holds
    entries - and it still totals to something publishable.

    The uniqueness invariant was asserted over well-formed fixtures, which is to say it
    was asserted where it could not fail. This drives it from a staged dimension whose
    intervals overlap, which is what a broken chain would produce."""
    import datetime

    raw_dir, staging = built()

    # Collected first: the frame is a lazy read of the path about to be overwritten,
    # and Spark would go looking for the file after it had gone.
    chained = scd2.frame(spark, DIM_CC, staging)
    schema, rows = chained.schema, [row.asDict() for row in chained.collect()]
    duplicate = {
        **rows[0],
        "surrogate_key": "duplicate",
        "valid_from": datetime.date(2020, 1, 1),
        "valid_to": datetime.date(9999, 12, 31),
    }
    spark.createDataFrame(rows + [duplicate], schema).coalesce(1).write.mode(
        "overwrite"
    ).parquet(str(scd2.staging_path(staging, DIM_CC)))

    with pytest.raises(facts.Multiplied) as failure:
        facts.build(spark, raw_dir, staging)
    assert "overlapping" in str(failure.value)


@pytest.mark.parametrize("missing", ["account", "cost_centre"])
def test_an_entry_with_no_dimension_version_fails_the_build(spark, built, missing):
    """Review finding 6. The gate was driven only through a missing rate; it guards
    three joins and each has to be able to trip it."""
    orphan = entry("E-NOWHERE", "2026-03-16", **(
        {"account_code": "999999"} if missing == "account"
        else {"cost_center_code": "CC-NONE"}
    ))
    with pytest.raises(facts.Unattributed) as failure:
        built(entries=ENTRIES + [orphan])
    assert "E-NOWHERE" in str(failure.value)


def test_the_product_keeps_its_full_precision_before_rounding(spark, built):
    """Review finding 4. The persisted column is rounded, so its type says nothing
    about the multiplication that produced it. 37 is one short of Spark's cap and at
    the cap Spark reduces the scale rather than raising - this asserts the intermediate
    exactly, which is where that would show."""
    from pyspark.sql import functions as F

    _, staging = built()
    product = facts.frame(spark, staging).select(
        (F.col("amount_dr") * F.col("rate_to_base")).alias("product")
    )
    assert dict(product.dtypes)["product"] == "decimal(37,8)"


# --- the adjustment fact (ADR 0042) -----------------------------------------
#
# Cases 17-21 of task.md. `gl_adjustment` has been ingested since the incremental load
# landed and consumed by nothing. It becomes a fact of its own rather than rows in
# `fct_gl_entry`, because gate 3 requires the rows sharing a `doc_id` to balance and an
# adjustment is a single-sided delta against a voucher that already balanced.

GL_ADJUSTMENT = contracts.load("gl_adjustment")


def adjustment(entry_id, accounting_date, adjusts, kind, **overrides):
    row = {
        "entry_id": entry_id, "version": "2",
        "accounting_date": accounting_date, "posted_at": accounting_date,
        "account_code": "660204", "cost_center_code": "CC-002",
        "currency": "EUR", "amount_dr": "100.00", "amount_cr": "0.00",
        "doc_id": entry_id, "adjusts_entry_id": adjusts, "adjustment_type": kind,
        "vendor_code": "V-0001", "description": "Adjustment",
    }
    row.update(overrides)
    return row


ADJUSTMENTS = [
    adjustment("A-CORR-000000", "2026-03-16", "E-MAR", "correction"),
    adjustment("A-REST-000000", "2026-07-15", "E-JUL", "restatement"),
]


@pytest.fixture
def built_with_adjustments(tmp_path, spark):
    def build(entries=ENTRIES, adjustments=ADJUSTMENTS, accounts=ACCOUNTS,
              centres=COST_CENTRES, rates=RATES):
        raw_dir, staging = tmp_path / "raw", tmp_path / "staging"
        raw.merge_table(GL_ENTRY, raw_dir, entries, run_id=RUN_A)
        raw.merge_table(GL_ADJUSTMENT, raw_dir, adjustments, run_id=RUN_A)
        raw.merge_table(DIM_ACCOUNT, raw_dir, accounts, run_id=RUN_A)
        raw.merge_table(DIM_CC, raw_dir, centres, run_id=RUN_A)
        raw.merge_table(DIM_FX, raw_dir, rates, run_id=RUN_A)
        for contract in (DIM_ACCOUNT, DIM_CC, DIM_FX):
            scd2.build(spark, contract, raw_dir, staging)
        facts.build(spark, raw_dir, staging)
        facts.build(spark, raw_dir, staging, model=facts.ADJUSTMENT_MODEL)
        return raw_dir, staging
    return build


def adjustments_by_id(spark, staging):
    return {
        row["entry_id"]: row
        for row in facts.read(spark, staging, model=facts.ADJUSTMENT_MODEL)
    }


def test_the_adjustment_fact_is_built_and_carries_its_own_columns(spark,
                                                                  built_with_adjustments):
    """Case 17. One row per adjustment, and the two columns that make it an adjustment
    rather than an entry survive into staging."""
    _, staging = built_with_adjustments()
    landed = adjustments_by_id(spark, staging)

    assert set(landed) == {"A-CORR-000000", "A-REST-000000"}
    assert landed["A-CORR-000000"]["adjusts_entry_id"] == "E-MAR"
    assert landed["A-CORR-000000"]["adjustment_type"] == "correction"
    assert landed["A-REST-000000"]["adjustment_type"] == "restatement"


def test_an_adjustment_is_attributed_by_the_same_three_joins(spark,
                                                             built_with_adjustments):
    """Case 18. As of its own accounting date, not today's structure: the July
    adjustment gets the department the cost centre moved to, the March one does not."""
    _, staging = built_with_adjustments()
    landed = adjustments_by_id(spark, staging)

    for row in landed.values():
        assert row["account_key"] is not None
        assert row["cost_center_key"] is not None
        assert row["fx_key"] is not None

    assert landed["A-CORR-000000"]["dept_code"] == "DEPT-RND"
    assert landed["A-REST-000000"]["dept_code"] == "DEPT-OPS"


def test_an_adjustment_converts_and_rounds_at_the_line(spark, built_with_adjustments):
    """Case 19. The same rule as an entry: docs/adr/0031 rounds at the line so a total
    of the detail is the number the report shows."""
    _, staging = built_with_adjustments()
    landed = adjustments_by_id(spark, staging)

    row = landed["A-CORR-000000"]
    assert row["rate_to_base"] == Decimal("7.800000")
    assert row["amount_dr_base"] == Decimal("780.00")


def test_adjustments_are_absent_from_the_entry_fact(spark, built_with_adjustments):
    """Case 20. Folding them in would turn gate 3 red on correct data, because an
    adjustment's doc_id has one side."""
    _, staging = built_with_adjustments()

    assert not [row for row in facts.read(spark, staging)
                if row["entry_id"].startswith("A-")]


def test_an_unattributable_adjustment_raises(spark, built_with_adjustments):
    """Case 21. The same gate as an entry: a dimension gap drops rows out of an inner
    join with nothing raised, so the count is checked rather than trusted."""
    orphan = adjustment("A-CORR-000001", "2019-01-01", "E-MAR", "correction")

    with pytest.raises(facts.Unattributed):
        built_with_adjustments(adjustments=[*ADJUSTMENTS, orphan])


# --- partitioned, and selectively written (ADR 0026, 0041) ------------------
#
# Cases 22-26 of task.md. A partition is written by overwriting its own directory, not
# by `partitionBy` over the whole frame: Spark's `partitionOverwriteMode` defaults to
# STATIC, under which an overwrite replaces the entire table directory.


def partition_mtimes(staging, model=None) -> dict[str, int]:
    """Every partition directory's newest file modification time, in nanoseconds."""
    root = facts.staging_path(staging, model=model or facts.MODEL)
    found = {}
    for directory in sorted(root.glob("accounting_period=*")):
        period = directory.name.split("=", 1)[1]
        found[period] = max(
            path.stat().st_mtime_ns for path in directory.iterdir() if path.is_file()
        )
    return found


SPREAD = [
    entry("E-JAN", "2026-01-15"),
    entry("E-MAR", "2026-03-16"),
    entry("E-JUN", "2026-06-15"),
    entry("E-JUL", "2026-07-15"),
]


def test_the_entry_fact_is_partitioned_by_accounting_period(spark, built):
    """Case 22. One directory per period present in raw, matching the raw layout."""
    raw_dir, staging = built(entries=SPREAD)

    landed = set(partition_mtimes(staging))
    assert landed == {"2026-01", "2026-03", "2026-06", "2026-07"}


def test_a_dirty_build_rewrites_only_the_dirty_partition(spark, built, tmp_path):
    """Case 23. The acceptance criterion, at the fact layer: the other partitions'
    files are not touched, which a modification time is what records."""
    raw_dir, staging = built(entries=SPREAD)
    before = partition_mtimes(staging)

    facts.build(spark, raw_dir, staging, dirty={"2026-03"})
    after = partition_mtimes(staging)

    assert after["2026-03"] != before["2026-03"]
    for period in ("2026-01", "2026-06", "2026-07"):
        assert after[period] == before[period], period


def test_a_selective_rebuild_equals_a_full_rebuild(spark, built, tmp_path):
    """Case 24. Selective recomputation is required to be indistinguishable from the
    rebuild - that is what makes it an optimisation rather than a second answer."""
    raw_dir, staging = built(entries=SPREAD)

    late = entry("E-MAR-LATE", "2026-03-20", posted_at="2026-05-30")
    raw.merge_table(GL_ENTRY, raw_dir, [late], run_id=RUN_A)

    facts.build(spark, raw_dir, staging, dirty={"2026-03"})
    selective = sorted(facts.read(spark, staging), key=lambda row: row["entry_id"])

    other = tmp_path / "full"
    for contract in (DIM_ACCOUNT, DIM_CC, DIM_FX):
        scd2.build(spark, contract, raw_dir, other)
    facts.build(spark, raw_dir, other)
    full = sorted(facts.read(spark, other), key=lambda row: row["entry_id"])

    assert facts.checksum(selective) == facts.checksum(full)


def test_no_dirty_set_writes_every_partition(spark, built):
    """Case 25. The behaviour before this change, and what a full rebuild still is."""
    raw_dir, staging = built(entries=SPREAD)
    before = partition_mtimes(staging)

    facts.build(spark, raw_dir, staging, dirty=None)
    after = partition_mtimes(staging)

    assert set(after) == set(before)
    for period in before:
        assert after[period] != before[period], period


def test_a_period_that_emptied_out_loses_its_partition(spark, built, tmp_path):
    """Case 26. A zero-row file would make the layout claim a period exists that holds
    nothing, which is `raw.write_partition`'s rule and applies here for the same
    reason."""
    raw_dir, staging = built(entries=SPREAD)
    assert "2026-06" in partition_mtimes(staging)

    for path in raw.partitions(GL_ENTRY, raw_dir):
        if path.parent.name == "accounting_period=2026-06":
            path.unlink()

    facts.build(spark, raw_dir, staging, dirty={"2026-06"})

    assert "2026-06" not in partition_mtimes(staging)


# --- the command ------------------------------------------------------------

def test_the_command_does_not_stop_a_session_that_is_active_on_another_thread(
        spark, built):
    """Case 2. `python -m transform.spark.facts` used to infer whether the session was
    its to stop from `session.active()`, which is thread-local. Called from a thread that
    has none, it stopped the process's session out from under its owner. See
    docs/adr/0049."""
    from conftest import run_off_thread, session_is_stopped

    raw_dir, staging = built()

    code = run_off_thread(lambda: facts.main(
        ["--raw", str(raw_dir), "--staging", str(staging)]))

    assert code == 0
    assert not session_is_stopped(spark)
