"""The second of the three quality gates: six dbt tests over the mart.

A gate is defined by what it stops, so every one of the six has a scenario that turns
it red. A gate asserted only on well-formed data is `assert True` wearing a costume,
and this file exists to make that impossible: for each gate there is a clean build that
passes it and at least one constructed build that does not.

The scenarios plant their failure in the landing schema, between the load and the
build, or generate a ledger with one of the generator's switches on. Each test owns its
own pair of schemas, so a planted failure reaches nothing else.

Cases 29-59 of task.md. See docs/adr/0036 for the drift gate's window and band, and
docs/adr/0031 for why the amount tolerance is half a cent.
"""

from decimal import Decimal

import pytest

from conftest import AUDIT_SUFFIX

pytestmark = pytest.mark.db

# From dbt_project.yml. Restated here so a test asserting the band is reading the same
# numbers the gate is, and a change to one without the other fails case 55.
DRIFT_WINDOW = 5
DRIFT_TOLERANCE = Decimal("0.10")

FAR_FUTURE = "9999-12-31"


def assert_failed(build, *fragments: str) -> None:
    """The build failed, and one of the failing test nodes names this gate.

    Node names rather than console text: a gate is asserted by name, and a substring
    match against the log would pass on a message that merely mentioned it.
    """
    assert not build.ok, f"expected the build to fail\n{build.output}"
    failed = build.failed_tests()
    matching = [name for name in failed if all(f in name for f in fragments)]
    assert matching, f"no failing test matched {fragments}; failures were {sorted(failed)}"


def stored_failures(db, build, test_name: str) -> list[tuple]:
    """The rows the gate kept, from the store_failures audit table.

    Four of the six gates are one node covering several branches - the interval test
    checks overlaps, gaps, inverted intervals and duplicate current versions - so
    "the node failed" does not say which branch caught it. The stored rows do.
    """
    with db.cursor() as cursor:
        cursor.execute(
            f'SELECT * FROM "{build.mart}{AUDIT_SUFFIX}"."{test_name}"'
        )
        return cursor.fetchall()


def sql(*statements: str):
    """A mutation to run against the landing schema before the build."""
    def mutate(db, schema: str) -> None:
        with db.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement.format(schema=schema))
        db.commit()
    return mutate


def duplicate_a_row(table: str):
    return sql(f'INSERT INTO "{{schema}}".{table} SELECT * FROM "{{schema}}".{table} LIMIT 1')


# --- gate 1: primary key uniqueness ----------------------------------------

def test_a_clean_build_passes_every_key_test(mart, clean_staging):
    """29. All seven models, uniqueness and not-null."""
    build = mart(clean_staging)
    assert build.ok, build.output


def test_a_duplicate_entry_fails_the_build(mart, clean_staging):
    """30. `(entry_id, version)` is what makes the merge idempotent upstream; a mart
    holding it twice reports every one of that entry's amounts twice."""
    build = mart(clean_staging, mutate=duplicate_a_row("fct_gl_entry"))
    assert_failed(build, "unique", "fct_gl_entry")


def test_a_duplicate_aggregate_row_fails_the_build(mart, clean_staging):
    """31. The aggregate's key is compound, and a uniqueness test on one column of it
    would pass exactly the duplicate that matters."""
    build = mart(clean_staging, mutate=duplicate_a_row("agg_monthly_balance"))
    assert_failed(build, "unique", "agg_monthly_balance")


@pytest.mark.parametrize("dimension", ["dim_account", "dim_cost_center",
                                       "dim_fx_rate", "dim_vendor"])
def test_a_duplicate_dimension_key_fails_the_build(mart, clean_staging, dimension):
    """32. One scenario per dimension. A gate claimed for four tables and tested on one
    is a claim about the other three that nothing checks."""
    build = mart(clean_staging, mutate=duplicate_a_row(dimension))
    assert_failed(build, "unique", dimension)


def test_a_null_key_fails_separately_from_uniqueness(mart, clean_staging):
    """33. Two different failures. A null key is not caught by a uniqueness test - one
    null does not repeat - and a table keyed on nothing joins to everything."""
    build = mart(
        clean_staging,
        mutate=sql('UPDATE "{schema}".fct_gl_entry SET entry_id = NULL '
                   'WHERE ctid IN (SELECT ctid FROM "{schema}".fct_gl_entry LIMIT 1)'),
    )
    assert_failed(build, "not_null", "fct_gl_entry")


# --- gate 2: referential integrity -----------------------------------------

def test_a_clean_build_passes_every_relationship(mart, clean_staging):
    """34. All four: the three surrogate keys and the vendor code."""
    build = mart(clean_staging)
    assert build.ok, build.output


@pytest.mark.parametrize(
    "dimension, key, gate",
    [
        ("dim_account", "account_key", "account_key"),
        ("dim_cost_center", "cost_center_key", "cost_center_key"),
        ("dim_fx_rate", "fx_key", "fx_key"),
    ],
)
def test_deleting_a_referenced_dimension_row_fails_the_build(
    mart, clean_staging, dimension, key, gate
):
    """35-37. One scenario per surrogate key. A fact row pointing at a dimension row
    that is not there carries a null name after the widening and an amount that no
    longer has a hierarchy to roll up into."""
    build = mart(
        clean_staging,
        mutate=sql(
            f'DELETE FROM "{{schema}}".{dimension} WHERE surrogate_key IN '
            f'(SELECT {key} FROM "{{schema}}".fct_gl_entry LIMIT 1)'
        ),
    )
    assert_failed(build, "relationships", gate)


def test_deleting_a_referenced_vendor_fails_the_build(mart, clean_staging):
    """38. The vendor join is on `vendor_code`, not a surrogate key - `facts.py` does
    not attribute a vendor - so this relationship is a different shape from the other
    three and is tested as one."""
    build = mart(
        clean_staging,
        mutate=sql(
            'DELETE FROM "{schema}".dim_vendor WHERE vendor_code IN '
            '(SELECT vendor_code FROM "{schema}".fct_gl_entry '
            ' WHERE vendor_code IS NOT NULL LIMIT 1)'
        ),
    )
    assert_failed(build, "relationships", "vendor_code")


def test_a_null_vendor_is_not_a_broken_reference(mart, clean_staging, db):
    """39. A sale has no supplier. A relationship test that failed on null would fail
    on every revenue entry in a correct ledger."""
    build = mart(clean_staging)
    assert build.ok, build.output

    with db.cursor() as cursor:
        cursor.execute(
            f'SELECT count(*) FROM "{build.mart}".fct_gl_entry WHERE vendor_code IS NULL'
        )
        assert cursor.fetchone()[0] > 0, "this dataset needs entries with no vendor"


# --- gate 3: debit and credit balance per voucher --------------------------

def test_a_clean_build_has_balanced_vouchers(mart, clean_staging, db):
    """40. Every `doc_id` has equal debit and credit totals. The contract for
    `gl_entry` says explicitly that this rule belongs to the dbt tests, because it is a
    rule about a group of rows."""
    build = mart(clean_staging)
    assert build.ok, build.output

    with db.cursor() as cursor:
        cursor.execute(
            f"""SELECT count(*) FROM (
                  SELECT doc_id FROM "{build.mart}".fct_gl_entry GROUP BY doc_id
                  HAVING sum(amount_dr_base) <> sum(amount_cr_base)) AS broken"""
        )
        assert cursor.fetchone()[0] == 0


def test_the_generators_dirty_vouchers_fail_the_build(mart, unbalanced_staging):
    """41. Not a planted row: the generator's own switch, which produces the shape a
    real broken export has."""
    build = mart(unbalanced_staging)
    assert_failed(build, "balanced")


def test_the_offending_voucher_is_readable_afterwards(mart, unbalanced_staging, db):
    """42. `store_failures` puts the failing rows in an audit table. A gate that can
    only say that something failed, not what, is half a gate - and a failing singular
    dbt test reports the node and a count, never the rows."""
    build = mart(unbalanced_staging)
    assert not build.ok, build.output

    audit = f"{build.mart}_dbt_test__audit"
    with db.cursor() as cursor:
        cursor.execute(
            """SELECT table_name FROM information_schema.tables
               WHERE table_schema = %s AND table_name LIKE %s""",
            (audit, "%balanced%"),
        )
        tables = [name for (name,) in cursor.fetchall()]
        assert tables, f"no stored failures in {audit}"
        cursor.execute(f'SELECT doc_id FROM "{audit}"."{tables[0]}"')
        offending = [doc_id for (doc_id,) in cursor.fetchall()]

    assert offending


def test_the_balance_gate_reads_base_currency(mart, clean_staging, db):
    """43. Base and original are different numbers, so which one the gate reads is a
    real choice rather than a spelling.

    A foreign-currency voucher balances in both, at a different figure in each. Base is
    the right one to test because it is what every consumer of this table adds up - the
    monthly aggregate sums `amount_dr_base`, and a gate that agreed with the original
    amounts while the base ones drifted would pass the ledger that gets reported."""
    build = mart(clean_staging)
    assert build.ok, build.output

    with db.cursor() as cursor:
        cursor.execute(
            f"""SELECT count(*) FROM "{build.mart}".fct_gl_entry
                WHERE currency <> 'CNY' AND amount_dr <> amount_dr_base
                  AND amount_dr > 0"""
        )
        assert cursor.fetchone()[0] > 0, "this dataset needs a foreign-currency entry"

        cursor.execute(
            f"""SELECT count(*) FROM (
                  SELECT doc_id FROM "{build.mart}".fct_gl_entry
                  WHERE currency <> 'CNY' GROUP BY doc_id
                  HAVING sum(amount_dr_base) <> sum(amount_cr_base)) AS broken"""
        )
        assert cursor.fetchone()[0] == 0


# --- gate 4: SCD2 interval consistency -------------------------------------

def test_a_clean_build_has_consistent_intervals(mart, clean_staging):
    """44. All three dimensions: intervals abut, none overlap, none leave a gap,
    exactly one row per natural key ends at the sentinel, and `valid_from <=
    valid_to`."""
    build = mart(clean_staging)
    assert build.ok, build.output


@pytest.mark.parametrize(
    "dimension, natural_key",
    [("dim_account", "account_code"), ("dim_cost_center", "cc_code"),
     ("dim_fx_rate", "currency")],
)
def test_an_overlapping_interval_fails_the_build(mart, clean_staging, db, dimension,
                                                 natural_key):
    """45. One scenario per dimension. An overlap is what `facts.py` raises
    `Multiplied` for: every entry matches both versions, which does not raise in the
    join - it multiplies, and the total is still a number somebody might publish."""
    build = mart(
        clean_staging,
        mutate=sql(
            f'UPDATE "{{schema}}".{dimension} SET valid_to = DATE \'{FAR_FUTURE}\' '
            f'WHERE valid_to < DATE \'{FAR_FUTURE}\' AND {natural_key} IN '
            f'(SELECT {natural_key} FROM "{{schema}}".{dimension} '
            f' GROUP BY {natural_key} HAVING count(*) > 1 LIMIT 1)'
        ),
    )
    assert_failed(build, "intervals", dimension)
    problems = {row[-1] for row in stored_failures(
        db, build, f"intervals_are_consistent_{dimension}")}
    assert any("overlaps" in problem for problem in problems), problems


def test_a_gap_between_versions_fails_the_build(mart, clean_staging, db):
    """46. A gap is the quieter failure: an entry dated inside it matches no version
    and drops out of an inner join with nothing raised."""
    build = mart(
        clean_staging,
        mutate=sql(
            'UPDATE "{schema}".dim_cost_center '
            "SET valid_to = valid_to - interval '10 days' "
            f"WHERE valid_to < DATE '{FAR_FUTURE}'"
        ),
    )
    assert_failed(build, "intervals", "dim_cost_center")
    problems = {row[-1] for row in stored_failures(
        db, build, "intervals_are_consistent_dim_cost_center")}
    assert any("gap" in problem for problem in problems), problems


def test_two_current_versions_fail_the_build(mart, clean_staging, db):
    """47. Exactly one row per natural key ends at the sentinel. Two of them is what a
    point-in-time join multiplies on, for every entry dated after the later one."""
    build = mart(
        clean_staging,
        mutate=sql(
            f'UPDATE "{{schema}}".dim_account SET valid_to = DATE \'{FAR_FUTURE}\' '
            f'WHERE ctid IN (SELECT ctid FROM "{{schema}}".dim_account '
            f' WHERE valid_to < DATE \'{FAR_FUTURE}\' LIMIT 1)'
        ),
    )
    assert_failed(build, "intervals", "dim_account")
    # The mutation also creates an overlap, so the node would fail either way. What has
    # to be shown is that the duplicate-current branch is the one that caught it.
    problems = {row[-1] for row in stored_failures(
        db, build, "intervals_are_consistent_dim_account")}
    assert any("current versions" in problem for problem in problems), problems


def test_an_inverted_interval_fails_the_build(mart, clean_staging, db):
    """48. `valid_from > valid_to` matches nothing at all, so the version silently
    stops existing rather than producing a wrong answer that looks wrong."""
    build = mart(
        clean_staging,
        mutate=sql(
            'UPDATE "{schema}".dim_account SET valid_to = valid_from - interval \'1 day\' '
            'WHERE ctid IN (SELECT ctid FROM "{schema}".dim_account LIMIT 1)'
        ),
    )
    assert_failed(build, "intervals", "dim_account")


# --- gate 5: row-count drift -----------------------------------------------

def test_the_gate_is_quiet_until_its_baseline_fills(mart, clean_staging):
    """49. It does not fire on the second build of a fresh clone. A gate whose first
    real use is a false alarm is a gate people learn to ignore.

    The gate has to have run and passed, not to have been absent: a drift test that
    simply is not in the graph would also leave the build green."""
    build = mart(clean_staging)
    assert build.ok, build.output

    drift = {name: status for name, status in build.statuses().items() if "drift" in name}
    assert drift, f"the drift test did not run; nodes were {sorted(build.statuses())}"
    assert set(drift.values()) == {"pass"}


def test_an_unchanged_count_is_not_drift(mart, clean_staging, db):
    """50. With a full baseline of stable counts, a clean build passes. The gate has to
    be capable of staying quiet, or it is not a gate, it is a failure."""
    first = mart(clean_staging)
    assert first.ok, first.output
    with db.cursor() as cursor:
        cursor.execute(f'SELECT count(*) FROM "{first.mart}".fct_gl_entry')
        count = cursor.fetchone()[0]
    seed_into_mart(db, first.mart, "fct_gl_entry", count)

    again = mart(clean_staging)
    assert again.ok, again.output


def seed_into_mart(db, mart_schema: str, model: str, count: int,
                   builds: int = DRIFT_WINDOW) -> None:
    with db.cursor() as cursor:
        for index in range(builds):
            cursor.execute(
                f'INSERT INTO "{mart_schema}".model_row_count '
                "(invocation_id, built_at, model, row_count) "
                "VALUES (%s, now(), %s, %s)",
                (f"seeded-{index}", model, count),
            )
    db.commit()


def test_a_load_carrying_half_the_ledger_fails_the_build(mart, clean_staging, db):
    """51. The failure this gate is for: a load that lost a partition. Every other gate
    passes on half a ledger, because half a ledger is internally consistent.

    Whole periods, not a random sample. Both lines of a voucher carry the same
    accounting date, so removing periods leaves every remaining voucher balanced -
    and that matters more than it looks: `dbt build` skips a model's descendants when
    one of its tests fails, so a scenario that also broke the balance gate would skip
    `model_row_count` and the drift test with it. The gate would never run, and the
    test asserting it fires would be asserting something impossible."""
    first = mart(clean_staging)
    assert first.ok, first.output
    with db.cursor() as cursor:
        cursor.execute(f'SELECT count(*) FROM "{first.mart}".fct_gl_entry')
        full = cursor.fetchone()[0]
    seed_into_mart(db, first.mart, "fct_gl_entry", full)

    halved = mart(
        clean_staging,
        mutate=sql('DELETE FROM "{schema}".fct_gl_entry '
                   "WHERE accounting_date < DATE '2026-07-01'"),
    )
    assert_failed(halved, "drift")


@pytest.mark.parametrize("factor, should_pass", [(Decimal("1.05"), True),
                                                 (Decimal("1.15"), False)])
def test_the_band_is_tested_at_both_sides_of_its_edge(mart, clean_staging, db,
                                                      factor, should_pass):
    """52. Five percent passes and fifteen fails, at the default tolerance of ten. A
    gate tested only with a huge change says nothing about where its edge is."""
    build = mart(clean_staging)
    assert build.ok, build.output
    with db.cursor() as cursor:
        cursor.execute(f'SELECT count(*) FROM "{build.mart}".fct_gl_entry')
        actual = cursor.fetchone()[0]
    # A row count is an integer, so the baseline is one too and the ratio it produces is
    # near the target rather than on it. What the test needs is not the exact figure but
    # the side of the band the scenario lands on - asserted here, before the gate is
    # asked, so a dataset whose size moved could not quietly put both cases on one side.
    baseline = int(Decimal(actual) / factor)
    ratio = Decimal(actual) / Decimal(baseline)
    if should_pass:
        assert ratio < 1 + DRIFT_TOLERANCE, f"{ratio} is not inside the band"
    else:
        assert ratio > 1 + DRIFT_TOLERANCE, f"{ratio} is not outside the band"
    seed_into_mart(db, build.mart, "fct_gl_entry", baseline)

    again = mart(clean_staging)
    if should_pass:
        assert again.ok, again.output
    else:
        assert_failed(again, "drift")


def test_the_baseline_excludes_the_current_build(mart, clean_staging, db):
    """53. Included, the current count contributes to the median it is being compared
    against, which drags the baseline toward whatever happened and weakens the gate
    exactly when it should fire hardest.

    Asserted on what the gate itself recorded, not inferred from whether it fired. With
    five stable builds behind it and a halved current one the gate fires either way, so
    "it fired" proves nothing about the exclusion. `store_failures` keeps the row the
    gate wrote, and that row says how many builds its baseline covered and what median
    it compared against. Six builds, or a median dragged below the stable count, would
    mean the current build was inside its own baseline."""
    first = mart(clean_staging)
    assert first.ok, first.output
    with db.cursor() as cursor:
        cursor.execute(f'SELECT count(*) FROM "{first.mart}".fct_gl_entry')
        full = cursor.fetchone()[0]
    seed_into_mart(db, first.mart, "fct_gl_entry", full)

    halved = mart(
        clean_staging,
        mutate=sql('DELETE FROM "{schema}".fct_gl_entry '
                   "WHERE accounting_date < DATE '2026-07-01'"),
    )
    assert_failed(halved, "drift")

    with db.cursor() as cursor:
        cursor.execute(
            f'SELECT row_count, median_count, builds '
            f'FROM "{halved.mart}{AUDIT_SUFFIX}".row_counts_have_not_drifted '
            f"WHERE model = 'fct_gl_entry'"
        )
        row_count, median, builds = cursor.fetchone()

    assert builds == DRIFT_WINDOW, "the current build is inside its own baseline"
    assert median == full, "the median was dragged by the count being tested"
    assert row_count < full


def test_the_snapshot_does_not_count_itself(mart, clean_staging, db):
    """54. One row per counted model per build, carrying dbt's own `invocation_id`, and
    no row for `model_row_count`. A model counting its own output in the same
    invocation would depend on itself."""
    build = mart(clean_staging)
    assert build.ok, build.output

    with db.cursor() as cursor:
        cursor.execute(
            f'SELECT model, invocation_id FROM "{build.mart}".model_row_count'
        )
        rows = cursor.fetchall()

    models = [model for model, _ in rows]
    assert "model_row_count" not in models
    assert len(models) == len(set(models))
    assert len({invocation for _, invocation in rows}) == 1


def test_the_window_and_tolerance_are_vars(mart, clean_staging, db):
    """55. Declared in `dbt_project.yml` so a scenario can move them, and so that
    changing them is a decision rather than an edit to a query. Overriding the
    tolerance changes the outcome of case 52."""
    build = mart(clean_staging)
    assert build.ok, build.output
    with db.cursor() as cursor:
        cursor.execute(f'SELECT count(*) FROM "{build.mart}".fct_gl_entry')
        actual = cursor.fetchone()[0]
    seed_into_mart(db, build.mart, "fct_gl_entry", int(Decimal(actual) / Decimal("1.05")))

    tightened = mart(clean_staging, dbt_args=["--vars", "{drift_tolerance: 0.01}"])
    assert_failed(tightened, "drift")

    # And the window: widened past the history available, the same data passes, because
    # the gate goes quiet rather than comparing against a baseline it does not have.
    widened = mart(
        clean_staging,
        dbt_args=["--vars", "{drift_tolerance: 0.01, drift_window: 50}"],
    )
    assert widened.ok, widened.output


# --- gate 6: base and original amounts agree -------------------------------

def test_a_clean_build_has_agreeing_amounts(mart, clean_staging, db):
    """56. Both sides, every line. Not an independent recomputation of the conversion -
    the mart copies what Spark computed with the same rate - but a gate on the load
    path and on the numeric types."""
    build = mart(clean_staging)
    assert build.ok, build.output

    with db.cursor() as cursor:
        cursor.execute(
            f"""SELECT count(*) FROM "{build.mart}".fct_gl_entry
                WHERE abs(amount_dr_base - amount_dr * rate_to_base) > 0.005
                   OR abs(amount_cr_base - amount_cr * rate_to_base) > 0.005"""
        )
        assert cursor.fetchone()[0] == 0


def test_a_mangled_base_debit_fails_the_build(mart, clean_staging):
    """57. What a COPY that lost a decimal, or a column that arrived as a float, would
    look like."""
    build = mart(
        clean_staging,
        mutate=sql('UPDATE "{schema}".fct_gl_entry SET amount_dr_base = amount_dr_base + 1 '
                   'WHERE ctid IN (SELECT ctid FROM "{schema}".fct_gl_entry '
                   '               WHERE amount_dr > 0 LIMIT 1)'),
    )
    assert_failed(build, "amounts_agree")


def test_a_mangled_base_credit_fails_the_build(mart, clean_staging):
    """58. Separately. A test that checked debits alone would pass a mangled credit
    column, and half the ledger is credits."""
    build = mart(
        clean_staging,
        mutate=sql('UPDATE "{schema}".fct_gl_entry SET amount_cr_base = amount_cr_base + 1 '
                   'WHERE ctid IN (SELECT ctid FROM "{schema}".fct_gl_entry '
                   '               WHERE amount_cr > 0 LIMIT 1)'),
    )
    assert_failed(build, "amounts_agree")


def test_the_tolerance_is_load_bearing(mart, clean_staging, db):
    """59. The tolerance is the rounding docs/adr/0031 performs at the line, and a gate
    that fired on it would fire on correct data, every build.

    A planted half-cent is not constructible: `amount_dr_base` is numeric(32,2), so a
    value written into it is rounded to the cent before the gate ever sees it. What can
    be shown is the thing that matters - that a correct ledger really does carry
    discrepancies inside the tolerance, so the tolerance is what keeps the build green
    rather than a decoration on a comparison that would have held at zero."""
    build = mart(clean_staging)
    assert build.ok, build.output

    with db.cursor() as cursor:
        cursor.execute(
            f"""SELECT count(*) FROM "{build.mart}".fct_gl_entry
                WHERE abs(amount_dr_base - amount_dr * rate_to_base) > 0"""
        )
        inside = cursor.fetchone()[0]

    assert inside > 0, (
        "no line disagrees at all, so this gate would pass with a tolerance of zero "
        "and proves nothing about the tolerance it declares"
    )
