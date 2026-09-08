"""The mart: the same figures, in Postgres, shaped so the application computes nothing.

Nothing here recomputes anything. The staging layer already attributed each entry to
the structure in force on its accounting date and converted it at that date's rate, and
the property this layer has to establish is that none of that moved on the way in - per
account, per period, per entry.

The other property is reproducibility, and it has two readings that are not the same
claim. Three mart rebuilds over one fixed staging snapshot are identical in every
column. Three complete pipeline reruns are identical in every reported column, and
`source_last_run_id` moves - legitimately, because docs/adr/0018 has it set by whichever
run wrote the partition. Both are asserted. See docs/adr/0038.

Cases 13-28 of task.md.
"""

import re
from decimal import Decimal

import pytest

from test_mart_load import row_count, table_checksum

pytestmark = pytest.mark.db

PROVENANCE = ("source_first_run_id", "source_last_run_id")

# What ingest stamps on a row: a UTC timestamp and a short random suffix. Matched rather
# than merely checked non-empty, because `python -m ingest.runs` is what turns one of
# these into the window and the digest, and a value it cannot look up is not provenance.
RUN_ID = re.compile(r"\d{8}T\d{6}Z-[0-9a-f]{6}")

# The aggregate's measures, and the columns whose null means something. A comparison
# that skipped the null-bearing ones would pass a mart that lost every percentage.
AGGREGATE_COLUMNS = (
    "debit_total", "credit_total", "account_type",
    # All three bases, not just the restated one: a comparison that checked only the
    # number the report shows would pass a mart that had lost the as-reported view,
    # which is half of what this ticket built. See docs/adr/0043.
    "balance_as_reported", "restatement_delta", "balance_as_restated",
    "balance_delta_mom", "balance_pct_mom",
    "balance_delta_yoy", "balance_pct_yoy",
    "balance_rolling_3m",
)


def query(db, sql: str, params=()) -> list[tuple]:
    with db.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


def sums_by(db, schema: str, keys: str) -> dict:
    rows = query(
        db,
        f"""
        SELECT {keys}, sum(amount_dr_base), sum(amount_cr_base)
        FROM "{schema}".fct_gl_entry GROUP BY {keys}
        """,
    )
    return {row[:-2]: (row[-2], row[-1]) for row in rows}


def staging_sums_by(spark, staging, keys: tuple[str, ...]) -> dict:
    from transform.spark import facts

    totals: dict = {}
    for row in facts.read(spark, staging.staging):
        key = tuple(row[name] for name in keys)
        debit, credit = totals.get(key, (Decimal(0), Decimal(0)))
        totals[key] = (debit + row["amount_dr"] * 0 + row["amount_dr_base"],
                       credit + row["amount_cr_base"])
    return totals


# --- cases 13-15: the fact's figures did not move ---------------------------

def test_fact_agrees_with_staging_by_account(mart, clean_staging, db, spark):
    """13. Both sides. A comparison that checked debits alone would pass a mart that
    lost the credit column."""
    build = mart(clean_staging)

    assert sums_by(db, build.mart, "account_code") == staging_sums_by(
        spark, clean_staging, ("account_code",)
    )


def test_fact_agrees_with_staging_at_its_own_grain(mart, clean_staging, db, spark):
    """14. Per account, cost centre and period - not only rolled up to account. Two
    errors that cancel within an account survive the coarser check."""
    build = mart(clean_staging)
    keys = "account_code, cost_center_code, to_char(accounting_date, 'YYYY-MM')"
    in_mart = sums_by(db, build.mart, keys)

    from transform.spark import facts

    expected: dict = {}
    for row in facts.read(spark, clean_staging.staging):
        key = (row["account_code"], row["cost_center_code"],
               row["accounting_date"].strftime("%Y-%m"))
        debit, credit = expected.get(key, (Decimal(0), Decimal(0)))
        expected[key] = (debit + row["amount_dr_base"], credit + row["amount_cr_base"])

    assert in_mart == expected


def test_fact_holds_the_same_entries(mart, clean_staging, db, spark):
    """15. Same row count and the same set of keys: a mart that dropped an entry and
    gained a duplicate would balance on totals alone."""
    from transform.spark import facts

    build = mart(clean_staging)
    staged = {(row["entry_id"], row["version"])
              for row in facts.read(spark, clean_staging.staging)}
    landed = set(query(db, f'SELECT entry_id, version FROM "{build.mart}".fct_gl_entry'))

    assert landed == staged
    assert row_count(db, build.mart, "fct_gl_entry") == len(staged)


def test_aggregate_agrees_row_for_row(mart, clean_staging, db, spark):
    """16. Every measure, with null compared as null. `balance_pct_mom` is null when
    the prior balance was zero - which docs/adr/0033 says is the case an investigation
    cares about most - so a comparison that coalesced it would hide the column."""
    from transform.spark import balances

    build = mart(clean_staging)
    columns = ", ".join(AGGREGATE_COLUMNS)
    landed = {
        row[:3]: row[3:]
        for row in query(
            db,
            f"""SELECT account_code, cost_center_code, accounting_period, {columns}
                FROM "{build.mart}".agg_monthly_balance""",
        )
    }
    staged = {
        (row["account_code"], row["cost_center_code"], row["accounting_period"]):
            tuple(row[name] for name in AGGREGATE_COLUMNS)
        for row in balances.read(spark, clean_staging.staging)
    }

    assert landed == staged


# --- cases 17-20: what "the same run three times" means ---------------------

def test_three_rebuilds_over_one_snapshot_are_identical(mart, clean_staging, db):
    """17. Every column included. This is the property this task owns: the mart is a
    function of its inputs, and nothing in it is derived from when it ran."""
    reported = ("fct_gl_entry", "agg_monthly_balance", "dim_account",
                "dim_cost_center", "dim_fx_rate", "dim_vendor")

    first = mart(clean_staging)
    assert first.ok, first.output
    signature = {
        table: (row_count(db, first.mart, table), table_checksum(db, first.mart, table))
        for table in reported
    }

    for _ in range(2):
        again = mart(clean_staging)
        assert again.ok, again.output
        assert {
            table: (row_count(db, again.mart, table),
                    table_checksum(db, again.mart, table))
            for table in reported
        } == signature


def test_a_full_pipeline_rerun_leaves_the_figures_alone(mart, spark, db, tmp_path_factory):
    """18. `_last_run_id` moves on every rewritten partition - docs/adr/0018 - and
    `facts.py` carries it forward, so the provenance columns are excluded exactly as
    docs/adr/0017 excludes ingestion metadata from the raw checksum. The reported
    figures do not move, and that is what this asserts."""
    from conftest import build_staging

    signatures = []
    for _ in range(3):
        staging = build_staging(spark, tmp_path_factory.mktemp("rerun"))
        build = mart(staging)
        assert build.ok, build.output
        signatures.append(
            table_checksum(db, build.mart, "fct_gl_entry", exclude=PROVENANCE)
        )

        # Excluded from the checksum, not from the assertions. A column left out of the
        # comparison and then not looked at is a column nothing checks at all.
        for column in PROVENANCE:
            values = {
                value for (value,) in query(
                    db, f'SELECT DISTINCT {column} FROM "{build.mart}".fct_gl_entry'
                )
            }
            assert values and all(RUN_ID.fullmatch(value) for value in values), (
                f"{column} holds something that is not a run identifier: {values}"
            )

    assert len(set(signatures)) == 1


def test_the_row_count_snapshot_is_the_stated_exception(mart, clean_staging, db):
    """19. It accumulates one row per counted model per build, which is what it is for.
    Excluded from case 17 by name, and asserted here instead - the exclusion is a
    stated fact rather than a gap."""
    first = mart(clean_staging)
    assert first.ok, first.output
    models = len(set(query(db, f'SELECT model FROM "{first.mart}".model_row_count')))

    assert models > 0
    assert row_count(db, first.mart, "model_row_count") == models

    second = mart(clean_staging)
    assert second.ok, second.output
    assert row_count(db, second.mart, "model_row_count") == 2 * models


def test_the_checksum_ignores_physical_order(mart, clean_staging, db):
    """20. A table is a set of rows. Two tables holding the same rows in different
    physical order have to check the same, or the reproducibility criterion is
    asserting something about how Postgres happened to store them."""
    build = mart(clean_staging)
    original = table_checksum(db, build.mart, "dim_vendor")

    with db.cursor() as cursor:
        cursor.execute(
            f'CREATE TABLE "{build.mart}".dim_vendor_shuffled AS '
            f'SELECT * FROM "{build.mart}".dim_vendor ORDER BY random()'
        )
    db.commit()

    assert table_checksum(db, build.mart, "dim_vendor_shuffled") == original


# --- cases 21-26: the widening ---------------------------------------------

def test_the_fact_carries_its_names(mart, clean_staging, db):
    """21. The application queries this layer and computes nothing, so the names are
    here rather than in a join every query repeats."""
    build = mart(clean_staging)
    missing = query(
        db,
        f"""SELECT count(*) FROM "{build.mart}".fct_gl_entry
            WHERE account_name IS NULL OR cost_center_name IS NULL""",
    )[0][0]

    assert missing == 0
    assert row_count(db, build.mart, "fct_gl_entry") > 0


def test_the_vendor_name_is_the_right_one(mart, clean_staging, db):
    """22. Correct, not merely non-null. A join on the wrong key produces a name for
    every row and the wrong one for most of them."""
    build = mart(clean_staging)
    wrong = query(
        db,
        f"""SELECT count(*) FROM "{build.mart}".fct_gl_entry f
            JOIN "{build.mart}".dim_vendor v ON v.vendor_code = f.vendor_code
            WHERE f.vendor_name IS DISTINCT FROM v.name""",
    )[0][0]

    assert wrong == 0


def test_an_entry_without_a_vendor_survives_the_widening(mart, clean_staging, db):
    """23. A sale is earned from a customer, not paid to a supplier. The join is left,
    and an inner one here would silently drop every revenue entry."""
    build = mart(clean_staging)
    rows = query(
        db,
        f"""SELECT count(*), count(vendor_name) FROM "{build.mart}".fct_gl_entry
            WHERE vendor_code IS NULL""",
    )[0]

    assert rows[0] > 0
    assert rows[1] == 0


def test_the_aggregate_carries_names_and_no_vendor(mart, clean_staging, db):
    """24. Its grain has no vendor. A `vendor_name` there would be an invention, and
    an invention in the table the application reads is the worst place for one."""
    build = mart(clean_staging)
    columns = {
        name for (name,) in query(
            db,
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema = %s AND table_name = 'agg_monthly_balance'""",
            (build.mart,),
        )
    }

    assert {"account_name", "cost_center_name", "dept_code"} <= columns
    assert "vendor_name" not in columns


def test_the_aggregate_names_are_as_of_the_period_close(mart, clean_staging, db, spark):
    """25. The same as-of date `account_type` already uses. `balances.py` reports a
    month as of its end - docs/adr/0032 - and a name resolved at a different date would
    put two answers in one row."""
    from transform.spark import scd2

    build = mart(clean_staging)
    versions = query(
        db,
        f"""SELECT cc_code, count(*) FROM "{build.mart}".dim_cost_center
            GROUP BY cc_code HAVING count(*) > 1 LIMIT 1""",
    )
    assert versions, "this dataset needs a dimension with two versions to test the as-of"

    mismatched = query(
        db,
        f"""
        SELECT count(*)
        FROM "{build.mart}".agg_monthly_balance a
        JOIN "{build.mart}".dim_cost_center d ON d.cc_code = a.cost_center_code
        WHERE (to_date(a.accounting_period, 'YYYY-MM') + interval '1 month - 1 day')::date
              BETWEEN d.valid_from AND d.valid_to
          AND a.cost_center_name IS DISTINCT FROM d.name
        """,
    )[0][0]

    assert mismatched == 0


def test_widening_the_aggregate_does_not_multiply_it(mart, clean_staging, db, spark):
    """26. Over a dataset where a dimension has two versions in the reported range.
    Joining SCD2 dimensions on the natural key alone matches every version valid in any
    period, which does not raise - it multiplies, and the result still totals to
    something publishable."""
    from transform.spark import balances

    build = mart(clean_staging)
    staged = len(balances.read(spark, clean_staging.staging))

    assert row_count(db, build.mart, "agg_monthly_balance") == staged


def test_a_gap_over_a_period_end_is_caught_rather_than_labelled_blank(mart,
                                                                     clean_staging):
    """26a. The as-of join is a left join on a date range, so a dimension that leaves a
    gap over a period end produces a row with no name rather than no row. Left is right
    - an inner join would drop the figure instead - which is why the missing name has
    to be a gate rather than something a reader notices."""
    build = mart(
        clean_staging,
        mutate=_start_one_account_late,
    )
    assert not build.ok, build.output
    assert any(
        "account_name" in name or "cost_center_name" in name
        for name in build.failed_tests()
    ), sorted(build.failed_tests())


def _start_one_account_late(db, schema: str) -> None:
    """Move one account's earliest version to start mid-year.

    The intervals stay consistent - none overlap, none gap between versions, exactly one
    ends at the sentinel, and none is inverted - so the SCD2 gate has nothing to say.
    What changes is coverage: the period ends before June fall outside every interval
    for that account, which is precisely the shape a left range join answers with a
    null. Shrinking or shifting the whole chain instead would break the interval gate
    first, `dbt build` would skip the aggregate, and this gate would never run.

    The fact is unaffected: it joins the dimension on the surrogate key rather than by
    date, so only the aggregate's as-of join can produce this.
    """
    with db.cursor() as cursor:
        cursor.execute(
            f"""UPDATE "{schema}".dim_account
                SET valid_from = DATE '2026-06-01'
                WHERE account_code = (
                    -- One that the aggregate actually reports on. Most of the chart is
                    -- summary accounts nothing posts to, and moving one of those would
                    -- change no row in the grid.
                    SELECT a.account_code FROM "{schema}".dim_account a
                    JOIN "{schema}".agg_monthly_balance b
                      ON b.account_code = a.account_code
                    GROUP BY a.account_code HAVING count(DISTINCT a.valid_from) = 1
                    ORDER BY a.account_code LIMIT 1)"""
        )


# --- cases 27-28: provenance ------------------------------------------------

def test_no_mart_row_names_the_build_that_wrote_it(mart, clean_staging, db):
    """27. A column that changes every build cannot coexist with case 17. Which build
    wrote a table is reached from `model_row_count`. See docs/adr/0038."""
    build = mart(clean_staging)
    forbidden = {"dbt_invocation_id", "invocation_id", "built_at", "loaded_at",
                 "dbt_updated_at", "_dbt_run_id"}
    columns = {
        (table, name) for table, name in query(
            db,
            """SELECT table_name, column_name FROM information_schema.columns
               WHERE table_schema = %s AND table_name <> 'model_row_count'""",
            (build.mart,),
        )
    }

    assert not {name for _, name in columns} & forbidden


def test_the_fact_keeps_the_run_that_landed_the_source(mart, clean_staging, db, spark):
    """28. `source_last_run_id` names the ingest run, and `python -m ingest.runs` turns
    that into the window, the digest and the row counts. That is the whole trail."""
    from transform.spark import facts

    build = mart(clean_staging)
    staged = {
        (row["entry_id"], row["version"]):
            (row["source_first_run_id"], row["source_last_run_id"])
        for row in facts.read(spark, clean_staging.staging)
    }
    landed = {
        (entry_id, version): (first, last)
        for entry_id, version, first, last in query(
            db,
            f"""SELECT entry_id, version, source_first_run_id, source_last_run_id
                FROM "{build.mart}".fct_gl_entry""",
        )
    }

    assert landed == staged


# --- the adjustment fact and the two bases (ADR 0042, 0043) -----------------
#
# Cases 39-41 of task.md.


def test_the_adjustment_fact_is_widened_with_its_names(mart, clean_staging, db):
    """Case 39. The same widening as the entry fact: the application reads this layer
    and computes nothing, so the names are here rather than in a join every query
    repeats."""
    build = mart(clean_staging)

    assert row_count(db, build.mart, "fct_gl_adjustment") > 0
    missing = query(
        db,
        f"""SELECT count(*) FROM "{build.mart}".fct_gl_adjustment
            WHERE account_name IS NULL OR cost_center_name IS NULL""",
    )[0][0]
    assert missing == 0

    wrong = query(
        db,
        f"""SELECT count(*) FROM "{build.mart}".fct_gl_adjustment f
            JOIN "{build.mart}".dim_vendor v ON v.vendor_code = f.vendor_code
            WHERE f.vendor_name IS DISTINCT FROM v.name""",
    )[0][0]
    assert wrong == 0


def test_the_aggregate_exposes_both_bases_and_not_the_old_column(mart, clean_staging,
                                                                 db):
    """Case 40. Three columns in place of one, and the reader can add the delta to the
    reported figure and get the restated one."""
    build = mart(clean_staging)
    columns = {
        row[0]
        for row in query(
            db,
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema = %s AND table_name = 'agg_monthly_balance'""",
            (build.mart,),
        )
    }

    assert {"balance_as_reported", "restatement_delta", "balance_as_restated"} <= columns
    assert "balance" not in columns

    disagreeing = query(
        db,
        f"""SELECT count(*) FROM "{build.mart}".agg_monthly_balance
            WHERE balance_as_restated
                IS DISTINCT FROM balance_as_reported + restatement_delta""",
    )[0][0]
    assert disagreeing == 0
