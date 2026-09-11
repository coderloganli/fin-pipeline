"""The SCD2 dimension loader: effective-dated source rows into validity intervals.

The properties this module exists to establish, for *every* natural key in the output
rather than a sampled one: the intervals do not overlap, they leave no gap, and exactly
one of them is current. A defect that only affects the second key to change is exactly
the defect a sampled assertion misses.

The history is read rather than inferred. `dim_account_src` and `dim_cost_center_src`
key on `(code, effective_date)` and carry every version in every extract, so one run
over one extract reconstructs the whole history - and `valid_from` is the date the
change took effect in the business, not the date this pipeline noticed it.

See docs/adr/0023 (raw never forgets a key), 0024 (an open interval ends at a sentinel
date), 0025 (the surrogate key is derived), 0026 (staging is typed and rebuilt).

Cases 19-31 of task.md.
"""

import pytest

from ingest import contracts, raw
from transform.spark import scd2

DIM_CC = contracts.load("dim_cost_center_src")
DIM_ACCOUNT = contracts.load("dim_account_src")

RUN_A = "20260901T031500Z-aaaaaa"

FAR_FUTURE = "9999-12-31"

# One cost centre that never moves, and one that moves department in July - the
# scenario the point-in-time join exists for.
CC_ROWS = [
    {"cc_code": "CC-001", "name": "Sales - East", "dept_code": "DEPT-SALES",
     "effective_date": "2020-01-01"},
    {"cc_code": "CC-002", "name": "Sales - North", "dept_code": "DEPT-RND",
     "effective_date": "2020-01-01"},
    {"cc_code": "CC-002", "name": "Sales - North", "dept_code": "DEPT-OPS",
     "effective_date": "2026-07-01"},
]


@pytest.fixture
def landed(tmp_path):
    """A raw layer holding the cost-centre extract, and a staging directory to write
    into. The loader reads raw, so the fixture builds raw rather than a DataFrame."""
    def build(rows=CC_ROWS, contract=DIM_CC):
        raw_dir = tmp_path / "raw"
        raw.merge_table(contract, raw_dir, rows, run_id=RUN_A)
        return raw_dir, tmp_path / "staging"
    return build


def versions(rows, key_columns):
    """The output grouped by natural key, each group ordered by `valid_from`."""
    grouped = {}
    for row in rows:
        grouped.setdefault(tuple(row[name] for name in key_columns), []).append(row)
    for group in grouped.values():
        group.sort(key=lambda row: row["valid_from"])
    return grouped


# The digests docs/adr/0025's rendering produces for these versions, written out
# rather than recomputed. A helper that reapplied the formula would agree with an
# implementation that had the formula wrong in the same way; a literal cannot.
#
# The rendering, for anyone reproducing these: every field as `<length>:<value>`,
# joined by U+001F, then SHA-256. The length prefix is not decoration - the separator
# alone is not injective, because a source value may contain it. Same rule as
# `ingest.raw.checksum`, argued in that module.
KEY_CC002_2020 = "c16e5bdf6738f70ffd2e7770ad5ede16a8a450e1808be1ba77b68d898833e2b0"
KEY_CC002_2026 = "5b8ff346067ff1ae409bd953e21df777527520638695ffb9756b93beb755128b"
KEY_660204_2020 = "794ce60434e0146c2e1ff4d1db3aa5fbee3caead5749b995c63677a0b583dc7d"
KEY_660204_2026 = "8f5bcc3ab7c88b62ae7c6c84b1d20d06081ee65fe815d11f869555e15b778431"


# --- the intervals ---------------------------------------------------------

def test_a_key_with_one_version_gets_one_open_interval(spark, landed):
    """Case 19. `valid_to` is the sentinel rather than null: a null end makes BETWEEN
    evaluate to null, and the row drops out of an inner join with nothing raised. See
    docs/adr/0024."""
    raw_dir, staging = landed()
    scd2.build(spark, DIM_CC, raw_dir, staging)
    rows = scd2.read(spark, DIM_CC, staging)

    only = versions(rows, ["cc_code"])[("CC-001",)]
    assert len(only) == 1
    assert only[0]["valid_from"].isoformat() == "2020-01-01"
    assert only[0]["valid_to"].isoformat() == FAR_FUTURE
    assert only[0]["is_current"] is True


def test_a_key_that_changed_gets_two_abutting_intervals(spark, landed):
    """Case 20. The cost centre moved department on 1 July, so the version before it
    ends on 30 June - the day before, not the day of."""
    raw_dir, staging = landed()
    scd2.build(spark, DIM_CC, raw_dir, staging)
    rows = scd2.read(spark, DIM_CC, staging)

    moved = versions(rows, ["cc_code"])[("CC-002",)]
    assert len(moved) == 2
    assert [row["dept_code"] for row in moved] == ["DEPT-RND", "DEPT-OPS"]
    assert moved[0]["valid_from"].isoformat() == "2020-01-01"
    assert moved[0]["valid_to"].isoformat() == "2026-06-30"
    assert moved[1]["valid_from"].isoformat() == "2026-07-01"
    assert moved[1]["valid_to"].isoformat() == FAR_FUTURE
    assert [row["is_current"] for row in moved] == [False, True]


def test_no_natural_key_has_overlapping_intervals(spark, landed):
    """Case 21. Every key in the output, not a sampled one."""
    raw_dir, staging = landed()
    scd2.build(spark, DIM_CC, raw_dir, staging)

    for key, group in versions(scd2.read(spark, DIM_CC, staging), ["cc_code"]).items():
        for earlier, later in zip(group, group[1:]):
            assert earlier["valid_to"] < later["valid_from"], key


def test_no_natural_key_leaves_a_gap_between_intervals(spark, landed):
    """Case 22. Abutting exactly: the earlier interval's last day is the day before the
    later one's first. A gap is a date on which the dimension says nothing, and a fact
    on that date would attribute to nothing."""
    from datetime import timedelta

    raw_dir, staging = landed()
    scd2.build(spark, DIM_CC, raw_dir, staging)

    for key, group in versions(scd2.read(spark, DIM_CC, staging), ["cc_code"]).items():
        for earlier, later in zip(group, group[1:]):
            assert earlier["valid_to"] + timedelta(days=1) == later["valid_from"], key


def test_every_natural_key_has_exactly_one_current_version(spark, landed):
    """Case 23. Every key, for the reason case 21 is written that way."""
    raw_dir, staging = landed()
    scd2.build(spark, DIM_CC, raw_dir, staging)

    for key, group in versions(scd2.read(spark, DIM_CC, staging), ["cc_code"]).items():
        assert sum(1 for row in group if row["is_current"]) == 1, key


# --- the surrogate key -----------------------------------------------------

def test_the_surrogate_key_is_the_digest_the_decision_record_specifies(spark, landed):
    """Case 24. The expected value is written out rather than recomputed by the code
    under test, so a key that is stable and wrong fails here."""
    raw_dir, staging = landed()
    scd2.build(spark, DIM_CC, raw_dir, staging)

    moved = versions(scd2.read(spark, DIM_CC, staging), ["cc_code"])[("CC-002",)]
    assert moved[0]["surrogate_key"] == KEY_CC002_2020
    assert moved[1]["surrogate_key"] == KEY_CC002_2026


def test_two_versions_of_one_key_get_different_surrogate_keys(spark, landed):
    """Case 25. A key derived from the natural key alone would be stable across reruns
    and across repartitioning, and would collide here - which is what the fact table's
    foreign key exists to distinguish."""
    raw_dir, staging = landed()
    scd2.build(spark, DIM_CC, raw_dir, staging)

    moved = versions(scd2.read(spark, DIM_CC, staging), ["cc_code"])[("CC-002",)]
    assert moved[0]["surrogate_key"] != moved[1]["surrogate_key"]


def test_the_surrogate_key_does_not_depend_on_the_partition_count(spark, landed):
    """Case 26. `monotonically_increasing_id` is a function of the partition a row
    lands in, so the same input repartitioned would renumber the dimension and move
    every foreign key in the fact table. See docs/adr/0025."""
    setting = "spark.sql.shuffle.partitions"
    original = spark.conf.get(setting)
    seen = []
    try:
        for partitions in ("1", "7"):
            raw_dir, staging = landed()
            spark.conf.set(setting, partitions)
            scd2.build(spark, DIM_CC, raw_dir, staging)
            seen.append({
                (row["cc_code"], row["valid_from"]): row["surrogate_key"]
                for row in scd2.read(spark, DIM_CC, staging)
            })
    finally:
        # The session is shared for the whole run. Left at 7, this test decides what a
        # later one observes - which is how the configuration assertion below passed by
        # accident until the two were run on their own.
        spark.conf.set(setting, original)

    assert seen[0] == seen[1]


# --- what the attribute hash is for ----------------------------------------

def test_two_adjacent_versions_with_identical_attributes_collapse(spark, landed):
    """Case 27. A version that restates the previous one without changing anything is
    not a change, and counting it as one would make "how many versions does this key
    have" a function of how many times the source re-exported it."""
    repeated = CC_ROWS + [
        {"cc_code": "CC-001", "name": "Sales - East", "dept_code": "DEPT-SALES",
         "effective_date": "2026-03-01"},
    ]
    raw_dir, staging = landed(repeated)
    scd2.build(spark, DIM_CC, raw_dir, staging)

    unchanged = versions(scd2.read(spark, DIM_CC, staging), ["cc_code"])[("CC-001",)]
    assert len(unchanged) == 1
    assert unchanged[0]["valid_from"].isoformat() == "2020-01-01"
    assert unchanged[0]["valid_to"].isoformat() == FAR_FUTURE


def test_a_collapsed_version_does_not_shorten_the_interval_it_belongs_to(spark, landed):
    """Not in task.md's list. Found while probing the implementation: it is where the
    fingerprint collapse and the interval construction meet, and getting it wrong is
    silent.

    Three versions, the middle one restating the second without changing anything. The
    collapse has to happen before the interval is computed, so the surviving version
    runs through the redundant one to the day before the next real change. Collapsing
    afterwards would end it in 2024 and leave 2025 attributed to nothing - a gap that
    the every-key assertions would catch, but only if a chain this shape were built,
    and none of the other cases builds one."""
    chain = [
        {"cc_code": "CC-9", "name": "N", "dept_code": "A", "effective_date": "2020-01-01"},
        {"cc_code": "CC-9", "name": "N", "dept_code": "B", "effective_date": "2024-01-01"},
        {"cc_code": "CC-9", "name": "N", "dept_code": "B", "effective_date": "2025-01-01"},
        {"cc_code": "CC-9", "name": "N", "dept_code": "C", "effective_date": "2026-01-01"},
    ]
    raw_dir, staging = landed(chain)
    scd2.build(spark, DIM_CC, raw_dir, staging)

    versioned = versions(scd2.read(spark, DIM_CC, staging), ["cc_code"])[("CC-9",)]
    assert [row["dept_code"] for row in versioned] == ["A", "B", "C"]
    assert [row["valid_to"].isoformat() for row in versioned] == [
        "2023-12-31", "2025-12-31", FAR_FUTURE
    ]


# --- the account dimension -------------------------------------------------

ACCOUNT_ROWS = [
    {"account_code": "660204", "name": "Administrative expenses - office supplies",
     "parent_code": "6602", "account_type": "expense", "effective_date": "2020-01-01"},
    {"account_code": "660204", "name": "Administrative expenses - office supplies",
     "parent_code": "6601", "account_type": "expense", "effective_date": "2026-04-01"},
]


def test_an_account_that_changed_parent_is_chained_into_two_records(spark, landed):
    """Case 28. The same logic on the other dimension: a reclassification changes which
    first-level account a figure rolls up into, which is the chart-of-accounts twin of
    a cost centre moving department."""
    raw_dir, staging = landed(ACCOUNT_ROWS, DIM_ACCOUNT)
    scd2.build(spark, DIM_ACCOUNT, raw_dir, staging)

    chained = versions(scd2.read(spark, DIM_ACCOUNT, staging), ["account_code"])
    reclassified = chained[("660204",)]
    assert [row["parent_code"] for row in reclassified] == ["6602", "6601"]
    assert [row["surrogate_key"] for row in reclassified] == [
        KEY_660204_2020, KEY_660204_2026
    ]
    assert reclassified[0]["valid_to"].isoformat() == "2026-03-31"
    assert reclassified[1]["is_current"] is True


# --- the layer's own properties --------------------------------------------

def test_staging_carries_real_types_rather_than_text(spark, landed):
    """Case 29. Raw holds text because it has to answer whether the source really said
    that; staging is where the retyping happens, and a `valid_from` that stayed a string
    would push a cast to every consumer of a range join. See docs/adr/0026."""
    raw_dir, staging = landed()
    scd2.build(spark, DIM_CC, raw_dir, staging)

    types = dict(scd2.frame(spark, DIM_CC, staging).dtypes)
    assert types["valid_from"] == "date"
    assert types["valid_to"] == "date"
    assert types["is_current"] == "boolean"
    assert types["cc_code"] == "string"


def test_three_runs_over_one_raw_layer_reach_the_same_staging(spark, landed):
    """Case 30. Staging is overwritten on every run because it is derived; that is only
    safe if rebuilding it is a function of raw rather than of how many times it ran."""
    raw_dir, staging = landed()

    seen = []
    for _ in range(3):
        scd2.build(spark, DIM_CC, raw_dir, staging)
        rows = scd2.read(spark, DIM_CC, staging)
        seen.append((len(rows), scd2.checksum(rows, DIM_CC)))

    assert seen[0] == seen[1] == seen[2], seen


def test_a_version_no_longer_exported_still_takes_part_in_the_chain(spark, landed):
    """Case 31. The point of docs/adr/0023 reaching this far: raw keeps a version the
    current extract has stopped carrying, so the chain built from raw still closes the
    earlier interval at the right date."""
    raw_dir, staging = landed()
    raw.merge_table(DIM_CC, raw_dir, [CC_ROWS[2]], run_id=RUN_A)

    scd2.build(spark, DIM_CC, raw_dir, staging)
    moved = versions(scd2.read(spark, DIM_CC, staging), ["cc_code"])[("CC-002",)]

    assert len(moved) == 2
    assert moved[0]["valid_to"].isoformat() == "2026-06-30"


# --- the command -----------------------------------------------------------
#
# Found by the stage-8 review: the tests drove `build` directly and left the entry
# point somebody actually types uncovered.

def test_the_command_builds_every_dimension_it_is_given(spark, landed, tmp_path, capsys):
    """Case 10c. All three models in one invocation, and the printed line names where
    each landed. This asserted two lines until `fx_rate` became a model of its own -
    see docs/adr/0029."""
    raw_dir, staging = landed()
    raw.merge_table(DIM_ACCOUNT, raw_dir, ACCOUNT_ROWS, run_id=RUN_A)

    raw.merge_table(DIM_FX, raw_dir, FX_ROWS, run_id=RUN_A)

    code = scd2.main(["--raw", str(raw_dir), "--staging", str(staging)])

    assert code == 0
    assert capsys.readouterr().out.splitlines() == [
        f"dim_account_src: dim_account -> {staging / 'dim_account'}",
        f"dim_cost_center_src: dim_cost_center -> {staging / 'dim_cost_center'}",
        f"fx_rate: dim_fx_rate -> {staging / 'dim_fx_rate'}",
    ]
    assert len(scd2.read(spark, DIM_CC, staging)) == 3


def test_the_command_does_not_stop_a_session_it_was_handed(spark, landed):
    """`build` goes through `getOrCreate`, so the CLI is handed whatever is already
    running. Stopping that would take the suite's session away from every test after
    this one - which is exactly how this would be found without the assertion."""
    raw_dir, staging = landed()
    scd2.main(["--raw", str(raw_dir), "--staging", str(staging),
               "--table", "dim_cost_center_src"])

    assert not spark.sparkContext._jsc.sc().isStopped()


def test_the_command_does_not_stop_a_session_that_is_active_on_another_thread(
        spark, landed):
    """Case 1. The same claim as the test above, under the condition that breaks the
    inference the command used to make: the process has a session, and the thread calling
    `main` does not. `getActiveSession()` is thread-local, so it answered `None` here
    while `getOrCreate` handed the very session back - and the command stopped it. See
    docs/adr/0049."""
    from conftest import run_off_thread, session_is_stopped

    raw_dir, staging = landed()

    code = run_off_thread(lambda: scd2.main(
        ["--raw", str(raw_dir), "--staging", str(staging),
         "--table", "dim_cost_center_src"]))

    assert code == 0
    assert not session_is_stopped(spark)


def test_a_table_that_is_not_an_effective_dated_dimension_is_a_usage_error(tmp_path, capsys):
    """Exit 2, the code the ingest commands return for the same mistake - and returned
    before a session is built, so a typo does not pay for a JVM."""
    code = scd2.main(["--raw", str(tmp_path), "--staging", str(tmp_path),
                      "--table", "gl_entry"])

    assert code == 2
    assert "gl_entry" in capsys.readouterr().err


def test_the_session_is_configured_the_way_the_decision_record_argues(spark):
    """Case 6 of the review. Local mode with a small shuffle width is what makes this a
    library rather than a cluster; the timezone is pinned so a date does not depend on
    where the run happened. See docs/adr/0028."""
    assert spark.conf.get("spark.master").startswith("local")
    assert spark.conf.get("spark.sql.shuffle.partitions") == "4"
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


# --- the exchange rate is a dimension too -----------------------------------
#
# Cases 7-10c of task.md. `fx_rate` is loaded by this same code: a currency's rate runs
# from the day it was published until the day before the next one, so the weekend falls
# inside Friday's interval by construction rather than by a rule written for it.
# See docs/adr/0029.

DIM_FX = contracts.load("fx_rate")

# Thursday, Friday, Monday - three distinct values on purpose. A Friday equal to
# Thursday would be folded into Thursday's interval by the fingerprint collapse, and
# "Friday's interval" would then not be a thing to assert on. A non-base currency for
# the same reason: CNY is 1.000000 throughout and collapses to a single interval.
FX_ROWS = [
    {"currency": "EUR", "rate_date": "2026-01-01", "rate_to_base": "7.100000"},
    {"currency": "EUR", "rate_date": "2026-01-02", "rate_to_base": "7.200000"},
    {"currency": "EUR", "rate_date": "2026-01-05", "rate_to_base": "7.300000"},
    {"currency": "CNY", "rate_date": "2026-01-01", "rate_to_base": "1.000000"},
]


def test_every_currency_has_intervals_that_neither_overlap_nor_gap(spark, landed):
    """Case 7. The same three properties the dimensions are held to, for every currency
    in the output rather than a sampled one."""
    from datetime import timedelta

    raw_dir, staging = landed(FX_ROWS, DIM_FX)
    scd2.build(spark, DIM_FX, raw_dir, staging)

    for key, group in versions(scd2.read(spark, DIM_FX, staging), ["currency"]).items():
        for earlier, later in zip(group, group[1:]):
            assert earlier["valid_to"] < later["valid_from"], key
            assert earlier["valid_to"] + timedelta(days=1) == later["valid_from"], key
        assert sum(1 for row in group if row["is_current"]) == 1, key


def test_fridays_interval_covers_the_weekend(spark, landed):
    """Case 8. 2026-01-02 is a Friday and the next published day is Monday the 5th, so
    Friday's interval ends on Sunday the 4th. Nothing here is a weekend rule - it is
    the same "until the day before the next version" the dimensions use, and the
    weekend is simply what falls in the gap. See docs/adr/0029."""
    raw_dir, staging = landed(FX_ROWS, DIM_FX)
    scd2.build(spark, DIM_FX, raw_dir, staging)

    eur = versions(scd2.read(spark, DIM_FX, staging), ["currency"])[("EUR",)]
    friday = [row for row in eur if row["valid_from"].isoformat() == "2026-01-02"]
    assert len(friday) == 1
    assert friday[0]["valid_to"].isoformat() == "2026-01-04"


def test_a_rate_that_did_not_move_becomes_one_interval(spark, landed):
    """Case 9. Ordinary here, where it never happened on the dimensions: a currency
    that holds its rate for two days should not carry two versions saying the same
    thing."""
    steady = [
        {"currency": "EUR", "rate_date": "2026-01-01", "rate_to_base": "7.100000"},
        {"currency": "EUR", "rate_date": "2026-01-02", "rate_to_base": "7.100000"},
        {"currency": "EUR", "rate_date": "2026-01-05", "rate_to_base": "7.300000"},
    ]
    raw_dir, staging = landed(steady, DIM_FX)
    scd2.build(spark, DIM_FX, raw_dir, staging)

    eur = versions(scd2.read(spark, DIM_FX, staging), ["currency"])[("EUR",)]
    assert len(eur) == 2
    assert eur[0]["valid_from"].isoformat() == "2026-01-01"
    assert eur[0]["valid_to"].isoformat() == "2026-01-04"


def test_the_rate_contract_refuses_a_restated_day(tmp_path):
    """Case 10. A given day's rate does not change. build-scd2-dimensions left this one
    line to the ticket that first joins on it - see docs/adr/0023.

    The flag alone was what this asserted, which is a statement about a YAML file and
    not about the load. The refusal is the behaviour worth holding."""
    assert contracts.load("fx_rate").get("rows_are_immutable") is True

    raw_dir = tmp_path / "raw"
    raw.merge_table(DIM_FX, raw_dir, FX_ROWS, run_id=RUN_A)

    restated = [{**FX_ROWS[0], "rate_to_base": "7.999999"}]
    with pytest.raises(raw.ImmutableRowChanged) as failure:
        raw.merge_table(DIM_FX, raw_dir, restated, run_id=RUN_A)

    message = str(failure.value)
    assert "fx_rate" in message and "rate_to_base" in message
    assert "7.100000" in message and "7.999999" in message


def test_the_rate_lands_in_staging_as_a_decimal(spark, landed):
    """Case 10a. `docs/adr/0026` says staging is typed. The loader typed only the
    interval columns, which nothing noticed while every attribute in every model was a
    string - the rate is the first attribute that is not."""
    raw_dir, staging = landed(FX_ROWS, DIM_FX)
    scd2.build(spark, DIM_FX, raw_dir, staging)

    types = dict(scd2.frame(spark, DIM_FX, staging).dtypes)
    assert types["rate_to_base"].startswith("decimal"), types["rate_to_base"]
    assert types["currency"] == "string"


def test_typing_by_contract_leaves_the_string_dimensions_alone(spark, landed):
    """Case 10b. Both existing dimensions declare every attribute as a string, so
    casting by declared type has to be a no-op for them."""
    raw_dir, staging = landed()
    scd2.build(spark, DIM_CC, raw_dir, staging)

    types = dict(scd2.frame(spark, DIM_CC, staging).dtypes)
    assert types["cc_code"] == "string"
    assert types["dept_code"] == "string"
    assert types["name"] == "string"
