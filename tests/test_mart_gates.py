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
    build = mart(clean_staging, mutate=duplicate_a_row("gl_entry"))
    assert_failed(build, "unique", "fct_gl_entry")


# The aggregate's compound key is unique by construction: one row per combination per
# period, from a cross join over a distinct membership set. There is no shape a source
# table can take that produces a duplicate, so the gate stays (it guards the
# construction) and has no red path here. Said rather than left as a gap.


# One source table, one attribute changed, and the same effective date. The surrogate
# key is derived from `(natural key, valid_from)` - docs/adr/0025 - so two versions a
# source dates identically are two rows under one key. The fingerprint does not collapse
# them because their attributes differ, which is the point: a collapse hides a restated
# version, and this is a contradicted one.
CONFLICTING_VERSION = {
    "dim_account_src": 'SELECT account_code, name || \' (conflict)\', parent_code, '
                       'account_type, effective_date',
    "dim_cost_center_src": 'SELECT cc_code, name || \' (conflict)\', dept_code, '
                           'effective_date',
    "fx_rate": "SELECT currency, rate_date, (rate_to_base::numeric + 1)::text",
}


@pytest.mark.parametrize("source_table", sorted(CONFLICTING_VERSION))
def test_two_versions_dated_the_same_day_fail_the_build(mart, clean_staging, source_table):
    """32. One scenario per effective-dated dimension. A gate claimed for three tables
    and tested on one is a claim about the other two that nothing checks."""
    build = mart(
        clean_staging,
        mutate=sql(f'INSERT INTO "{{schema}}".{source_table} '
                   f'{CONFLICTING_VERSION[source_table]} '
                   f'FROM "{{schema}}".{source_table} LIMIT 1'),
    )
    assert not build.ok, build.output
    # Two gates, and both are correct about it: one key with two rows, and an interval
    # that neither abuts nor is distinct from its neighbour.
    assert_failed(build, "unique")
    assert_failed(build, "intervals_are_consistent")


def test_a_duplicate_vendor_fails_the_build(mart, clean_staging):
    """32a. A supplier is not effective-dated, so it is keyed on the code itself and a
    second row under one code is the whole of the failure."""
    build = mart(clean_staging, mutate=duplicate_a_row("dim_vendor"))
    assert_failed(build, "unique", "dim_vendor")


def test_a_null_key_fails_separately_from_uniqueness(mart, clean_staging):
    """33. Two different failures. A null key is not caught by a uniqueness test - one
    null does not repeat - and a table keyed on nothing joins to everything."""
    build = mart(
        clean_staging,
        mutate=sql('UPDATE "{schema}".gl_entry SET entry_id = NULL '
                   'WHERE ctid IN (SELECT ctid FROM "{schema}".gl_entry LIMIT 1)'),
    )
    assert_failed(build, "not_null", "fct_gl_entry")


# --- gate 2: referential integrity -----------------------------------------

def test_a_clean_build_passes_every_relationship(mart, clean_staging):
    """34. All four: the three surrogate keys and the vendor code."""
    build = mart(clean_staging)
    assert build.ok, build.output


# Deleting a referenced dimension row no longer produces an orphan key. The surrogate
# key is computed by the join, so an entry whose dimension version is gone matches
# nothing and drops out - silently, which is what gate 6 was added for. That scenario
# lives with gate 6 at the bottom of this file rather than here.


def test_deleting_a_referenced_vendor_fails_the_build(mart, clean_staging):
    """38. The vendor join is on `vendor_code`, not a surrogate key - `facts.py` does
    not attribute a vendor - so this relationship is a different shape from the other
    three and is tested as one."""
    build = mart(
        clean_staging,
        mutate=sql(
            'DELETE FROM "{schema}".dim_vendor WHERE vendor_code IN '
            '(SELECT vendor_code FROM "{schema}".gl_entry '
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
        mutate=sql('DELETE FROM "{schema}".gl_entry '
                   # The source table holds text, because the raw layer does. Typing is
                   # the first thing the models do, not something the landing copy did.
                   "WHERE accounting_date::date < DATE '2026-07-01'"),
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
        mutate=sql('DELETE FROM "{schema}".gl_entry '
                   # The source table holds text, because the raw layer does. Typing is
                   # the first thing the models do, not something the landing copy did.
                   "WHERE accounting_date::date < DATE '2026-07-01'"),
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

# --- gates over the adjustment fact (ADR 0042) ------------------------------
#
# Cases 42-45 of task.md. It carries the gates that apply to it - key uniqueness,
# referential integrity, and the amount agreement - and not gate 3, because it is not a
# voucher and never was.


def test_the_voucher_gate_stays_green_with_adjustments_in_the_mart(mart, clean_staging,
                                                                   db):
    """Case 45. The case that decided the shape. An adjustment is a single-sided delta
    against a voucher that already balanced, so folding it into `fct_gl_entry` would
    turn gate 3 red on a correct ledger - see docs/adr/0042."""
    build = mart(clean_staging)

    # The precondition is the whole test. Gate 3 is trivially green on a mart holding no
    # adjustments, so without this the case passes before the feature exists and asserts
    # nothing whatever.
    with db.cursor() as cursor:
        cursor.execute(f'SELECT count(*) FROM "{build.mart}".fct_gl_adjustment')
        adjustments = cursor.fetchone()[0]
    assert adjustments > 0, "no adjustments reached the mart; the case would be vacuous"

    assert build.ok, build.output
    assert not [name for name in build.failed_tests() if "vouchers_are_balanced" in name]


# --- gate 6: every entry is attributed --------------------------------------

def test_a_missing_dimension_row_drops_entries_and_the_gate_says_so(mart, clean_staging, db):
    """The guard the attribution used to raise for itself.

    Spark's fact build counted its own output and refused a build whose rows had
    multiplied or gone missing. The models join instead, and an inner join is silent
    about what it discarded - so the count moved into a gate. Deleting one cost centre
    from the source dimension leaves every entry on it matching no version, and they
    drop out with nothing said until this fires.
    """
    build = mart(
        clean_staging,
        mutate=sql('DELETE FROM "{schema}".dim_cost_center_src '
                   "WHERE cc_code = 'CC-005'"),
    )
    assert not build.ok, build.output
    assert_failed(build, "attributed")

    problems = {row[-1] for row in stored_failures(
        db, build, "every_entry_is_attributed")}
    assert problems == {"the join dropped lines"}, problems
