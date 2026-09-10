"""The ticket's own acceptance, end to end.

A late adjustment dated two periods back recomputes that period and the periods whose
windows read through it, and nothing else. A dimension version arriving with no
accompanying journal entry recomputes the periods its validity interval covers - which
is the gap docs/adr/0027 recorded and left open, and which before this ticket recomputed
nothing at all.

Cases 46-48 of task.md. See docs/adr/0039, 0040, 0041.
"""

import subprocess
import sys

import pytest

from conftest import TEST_PERIODS, Staging, build_staging
from ingest import affected, contracts, raw
from transform.spark import balances, facts

GL_ADJUSTMENT = contracts.load("gl_adjustment")
DIM_CC = contracts.load("dim_cost_center_src")

RUN_LATE = "20260901T041500Z-bbbbbb"


def staging_mtimes(staging_dir) -> dict[tuple[str, str], int]:
    """Every staging partition's newest file modification time, by model and period."""
    found = {}
    for model in ("fct_gl_entry", "fct_gl_adjustment", "agg_monthly_balance"):
        root = staging_dir / model
        for directory in sorted(root.glob("accounting_period=*")):
            period = directory.name.split("=", 1)[1]
            found[(model, period)] = max(
                path.stat().st_mtime_ns
                for path in directory.iterdir() if path.is_file()
            )
    return found


def run_backfill(raw_dir, staging_dir, periods=TEST_PERIODS):
    return subprocess.run(
        [sys.executable, "-m", "transform.backfill",
         "--raw", str(raw_dir), "--staging", str(staging_dir),
         "--periods", periods],
        capture_output=True, text=True,
    )


@pytest.fixture
def pipeline(spark, tmp_path_factory) -> Staging:
    """A full run with the late-entry and restatement switches on - the two switches
    the generator carries for this ticket."""
    return build_staging(
        spark, tmp_path_factory.mktemp("backfill"),
        late_entries=True, restatements=True,
        cost_centre_move=True, account_move=True,
    )


def test_a_late_adjustment_rewrites_only_the_periods_it_affects(spark, pipeline):
    """Case 46. The ticket's acceptance criterion, stated as it was written down:
    inject an adjustment dated two periods back and every other partition file's
    modification time is unchanged."""
    affected.clear(pipeline.raw)
    before = staging_mtimes(pipeline.staging)
    assert before, "the staging layer is not partitioned"

    # An account and cost centre the ledger already posts to. A pairing seen for the
    # first time is a change in the dense grid's membership, which correctly rewrites
    # every period - see docs/adr/0041 - and would make this assert the wrong thing.
    existing = next(
        row for row in facts.read(spark, pipeline.staging)
        if row["accounting_period"] < "2026-06"
    )
    late = {
        "entry_id": "A-LATE-000000", "version": "2",
        "accounting_date": "2026-06-15", "posted_at": "2026-08-20",
        "account_code": existing["account_code"],
        "cost_center_code": existing["cost_center_code"],
        "currency": existing["currency"], "amount_dr": "500.00", "amount_cr": "0.00",
        "doc_id": "A-LATE-000000", "adjusts_entry_id": existing["entry_id"],
        "adjustment_type": "correction", "vendor_code": "", "description": "late",
    }
    raw.merge_table(GL_ADJUSTMENT, pipeline.raw, [late], run_id=RUN_LATE)
    affected.record(pipeline.raw, periods=["2026-06"], versions=[])

    result = run_backfill(pipeline.raw, pipeline.staging)
    assert result.returncode == 0, result.stdout + result.stderr

    after = staging_mtimes(pipeline.staging)

    # The facts and the aggregate are scoped differently, and that is the design rather
    # than an accident. An entry's attribution is a function of that entry, so a fact
    # rewrites only the period whose rows changed. The aggregate's comparison columns
    # read through their windows, so it rewrites the closure of that period as well.
    # See docs/adr/0040 and 0041.
    dirty = {"2026-06"}
    closure = balances.dirty_closure(dirty, last_period="2026-12")
    assert closure == {"2026-06", "2026-07", "2026-08"}

    for (model, period), stamp in before.items():
        expected = closure if model == "agg_monthly_balance" else dirty
        if period in expected:
            assert after[(model, period)] != stamp, ("rewritten", model, period)
        else:
            assert after[(model, period)] == stamp, ("untouched", model, period)


def test_a_dimension_change_with_no_entry_still_recomputes(spark, pipeline):
    """Case 47. The docs/adr/0027 gap. A dimension change produces no entry, so under a
    mechanism driven by entries' accounting dates it triggered no recomputation at all
    and every downstream figure stayed at its old value."""
    affected.clear(pipeline.raw)
    before = staging_mtimes(pipeline.staging)

    held = list(raw.read_table(DIM_CC, pipeline.raw))
    existing = held[0]
    moved = {**existing, "name": "Renamed for case 47", "effective_date": "2026-05-01"}
    raw.merge_table(DIM_CC, pipeline.raw, [moved], run_id=RUN_LATE)
    affected.record(pipeline.raw, periods=[], versions=[
        {"table": "dim_cost_center_src", "key": [existing["cc_code"]],
         "effective_date": "2026-05-01"}
    ])

    state = affected.read(pipeline.raw)
    assert state.periods == []
    assert state.dimension_versions

    result = run_backfill(pipeline.raw, pipeline.staging)
    assert result.returncode == 0, result.stdout + result.stderr

    after = staging_mtimes(pipeline.staging)
    rewritten = {period for key, stamp in after.items()
                 if before.get(key) != stamp for _, period in [key]}
    assert rewritten, "a dimension change recomputed nothing - the ADR 0027 gap"

    # The version takes effect on 2026-05-01, so the periods from May onward that carry
    # entries on this cost centre are what the interval covers. Nothing before it is.
    assert {"2026-05", "2026-06"} <= rewritten
    assert "2026-01" not in rewritten

    # And the new version is what the fact is attributed against from then on. The
    # surrogate key, not the name: names are added by dbt, and the staging fact carries
    # the key the point-in-time join resolved. A key that did not move would mean the
    # rebuild reattributed nothing.
    keys = {
        row["accounting_period"]: row["cost_center_key"]
        for row in facts.read(spark, pipeline.staging)
        if row["cost_center_code"] == existing["cc_code"]
    }
    before_change = {p: k for p, k in keys.items() if p < "2026-05"}
    after_change = {p: k for p, k in keys.items() if p >= "2026-05"}
    assert before_change and after_change
    assert not (set(before_change.values()) & set(after_change.values()))


def test_a_dimension_version_on_an_unused_key_recomputes_nothing(spark, pipeline):
    """Case 47a. The intersection with activity is real, not decorative: a version for
    a cost centre that carries no entry anywhere is a no-op rather than a full
    rebuild."""
    affected.clear(pipeline.raw)
    before = staging_mtimes(pipeline.staging)

    unused = {"cc_code": "CC-UNUSED", "name": "Nowhere", "dept_code": "DEPT-OPS",
              "effective_date": "2026-05-01"}
    raw.merge_table(DIM_CC, pipeline.raw, [unused], run_id=RUN_LATE)
    affected.record(pipeline.raw, periods=[], versions=[
        {"table": "dim_cost_center_src", "key": ["CC-UNUSED"],
         "effective_date": "2026-05-01"}
    ])

    result = run_backfill(pipeline.raw, pipeline.staging)
    assert result.returncode == 0, result.stdout + result.stderr

    assert staging_mtimes(pipeline.staging) == before


def test_the_backfill_does_not_clear_the_state_file(spark, pipeline):
    """Case 46, the other half of docs/adr/0039: clearing is the orchestrator's own
    step, so a module that silently cleared shared state on success cannot be run twice
    or alone without consequences that are not visible where the command is typed."""
    affected.clear(pipeline.raw)
    affected.record(pipeline.raw, periods=["2026-06"], versions=[])

    run_backfill(pipeline.raw, pipeline.staging)

    assert affected.read(pipeline.raw).periods == ["2026-06"]

    subprocess.run([sys.executable, "-m", "ingest.affected",
                    "--raw", str(pipeline.raw), "--clear"], check=True)
    assert affected.read(pipeline.raw).periods == []


@pytest.mark.db
def test_a_dimension_change_reaches_the_mart(pipeline, mart, db):
    """Case 47, the half the staging assertions cannot make. The case asks for
    `mart.agg_monthly_balance.cost_center_name` to read the new value, and that is the
    figure an analyst would see change without one journal entry having been posted -
    which is the whole of what docs/adr/0027 said this platform could not do.
    """
    affected.clear(pipeline.raw)

    held = list(raw.read_table(DIM_CC, pipeline.raw))
    existing = held[0]
    renamed = "Renamed for case 47"
    moved = {**existing, "name": renamed, "effective_date": "2026-05-01"}
    raw.merge_table(DIM_CC, pipeline.raw, [moved], run_id=RUN_LATE)
    affected.record(pipeline.raw, periods=[], versions=[
        {"table": "dim_cost_center_src", "key": [existing["cc_code"]],
         "effective_date": "2026-05-01"}
    ])

    result = run_backfill(pipeline.raw, pipeline.staging)
    assert result.returncode == 0, result.stdout + result.stderr

    build = mart(pipeline)
    assert build.ok, build.output

    with db.cursor() as cursor:
        cursor.execute(
            f'''SELECT accounting_period, cost_center_name
                FROM "{build.mart}".agg_monthly_balance
                WHERE cost_center_code = %s''',
            (existing["cc_code"],),
        )
        names: dict[str, str] = {}
        for period, name in cursor.fetchall():
            names.setdefault(period, name)

    after = {p: n for p, n in names.items() if p >= "2026-05"}
    before = {p: n for p, n in names.items() if p < "2026-05"}

    assert after and set(after.values()) == {renamed}
    assert before and renamed not in set(before.values()), \
        "a period that closed before the change kept the name it reported at the time"
