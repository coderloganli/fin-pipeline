"""The daily run, end to end, and the three things this ticket was accepted on.

Running the pipeline used to be seven commands in a remembered order, and only the
second of them wrote anything down. These are the criteria that say that is over: the
same run three times changes nothing, a backdated adjustment rewrites only what it
affects, and a run that died can say where.

The third is the one the ticket was opened for. Asked which part hurt most, the answer
was not the typing - it was finishing a run and not knowing what came of it.

Cases 29-33 of orchestrate-the-daily-run. See docs/adr/0044 and 0046.
"""

import pytest

from conftest import (TEST_PERIODS, TEST_LANDING_SCHEMA, TEST_MART_SCHEMA,
                      drop_schemas_under, run_dbt, schema_for, schemas_under)
from ingest import affected, contracts, raw, runs
from pipeline import run as runner
from pipeline import steps as step_list
from test_mart_load import row_count, table_checksum
from test_mart_promotion import RESERVE as BUILD_RESERVE, baseline
from transform import promote
from transform.spark import balances

pytestmark = pytest.mark.db

GL_ADJUSTMENT = contracts.load("gl_adjustment")

MART_TABLES = ("fct_gl_entry", "fct_gl_adjustment", "agg_monthly_balance",
               "dim_account", "dim_cost_center", "dim_fx_rate", "dim_vendor")

# docs/adr/0038: the mart's provenance columns move on a rerun by design, so the
# checksum excludes them exactly as docs/adr/0017 excludes ingestion metadata.
PROVENANCE = ("source_first_run_id", "source_last_run_id")


@pytest.fixture
def daily(request, db, spark, tmp_path_factory):
    """A context pointed at a generated source and at schemas of this test's own."""
    from generator import generate
    from generator.config import Config

    root = tmp_path_factory.mktemp("daily")
    source = root / "source"
    generate(Config(seed=42, out_dir=source, periods=TEST_PERIODS,
                    entries_per_period=40, late_entries=True, restatements=True))

    landing = schema_for(request.node.nodeid, TEST_LANDING_SCHEMA)
    # Room for `__b<run_id>`: this fixture runs the real pipeline, so its mart schema is
    # what a build schema is derived from. Without the reserve, dbt's audit suffix pushes
    # the name past Postgres's 63 characters and it truncates silently.
    mart = schema_for(request.node.nodeid, TEST_MART_SCHEMA, reserve=BUILD_RESERVE)
    context = runner.Context(
        source_dir=source,
        raw_dir=root / "raw",
        staging_dir=root / "staging",
        periods=TEST_PERIODS,
        landing_schema=landing,
        mart_schema=mart,
        spark=spark,
    )

    yield context

    drop_schemas_under(db, landing, mart)


def mart_state(db, schema):
    return {
        table: (row_count(db, schema, table),
                table_checksum(db, schema, table, exclude=PROVENANCE))
        for table in MART_TABLES
    }


def detail_of(record, step):
    return next(one.detail for one in record.steps if one.step == step)


def staging_mtimes(staging_dir, model):
    found = {}
    for directory in sorted((staging_dir / model).glob("accounting_period=*")):
        found[directory.name.split("=", 1)[1]] = max(
            path.stat().st_mtime_ns for path in directory.iterdir() if path.is_file()
        )
    return found


# --- the acceptance criteria -----------------------------------------------

def test_the_same_run_three_times_changes_nothing(db, daily):
    """Case 29. The backlog entry's first criterion.

    Row counts and checksums are asserted, and so is that the second and third runs
    rebuilt no periods. Without the second assertion this would pass for the wrong
    reason: a run that tore the whole mart down and rebuilt it from scratch also leaves
    the counts and the checksums equal, and would tell us nothing about idempotency.
    """
    runner.run_pipeline(daily, command="daily", steps=step_list.DAILY)
    after_first = mart_state(db, daily.mart_schema)

    for _ in range(2):
        run_id = runner.run_pipeline(daily, command="daily", steps=step_list.DAILY)
        record = runs.RunLog(daily.raw_dir).read()[-1]

        assert record.run_id == run_id
        assert record.status == "succeeded"
        assert detail_of(record, "recompute")["periods"] == []
        assert mart_state(db, daily.mart_schema) == after_first


def test_a_backdated_adjustment_rewrites_only_what_it_affects(db, daily):
    """Case 30. The backlog entry's second criterion.

    Two different sets, asserted separately, because conflating them is how this test
    would silently check the wrong thing. `transform.backfill.run` returns the dirty set
    *before* the rolling-window closure and that is what `recompute` records, so the
    facts see only the entry's own period. `balances.build` applies `dirty_closure`
    itself when it chooses partitions, so the aggregate legitimately rewrites more.
    See docs/adr/0040 and 0041.
    """
    runner.run_pipeline(daily, command="daily", steps=step_list.DAILY)

    # An account and cost centre the ledger already posts to: a pairing seen for the
    # first time changes the dense grid's membership and correctly rewrites every
    # period - docs/adr/0041 - which would make this assert the wrong thing.
    from transform.spark import facts as fact_build
    existing = next(row for row in fact_build.read(daily.spark, daily.staging_dir)
                    if row["accounting_period"] < "2026-06")
    late = {
        "entry_id": "A-LATE-000001", "version": "1",
        "adjusts_entry_id": existing["entry_id"], "adjustment_type": "correction",
        "accounting_date": "2026-06-15", "posted_at": "2026-12-20",
        "account_code": existing["account_code"],
        "cost_center_code": existing["cost_center_code"],
        "currency": "CNY", "amount_dr": "1000.00", "amount_cr": "0.00",
        "doc_id": "D-LATE-000001", "vendor_code": "", "description": "Late correction",
    }
    append_source_row(daily.source_dir, GL_ADJUSTMENT, late)

    before_facts = staging_mtimes(daily.staging_dir, "fct_gl_adjustment")
    before_entries = staging_mtimes(daily.staging_dir, "fct_gl_entry")
    before_balances = staging_mtimes(daily.staging_dir, "agg_monthly_balance")

    runner.run_pipeline(daily, command="daily", steps=step_list.DAILY)
    record = runs.RunLog(daily.raw_dir).read()[-1]

    # The fact periods: the adjustment's own accounting period, and nothing else.
    assert set(detail_of(record, "recompute")["periods"]) == {"2026-06"}

    for model, before in (("fct_gl_adjustment", before_facts),
                          ("fct_gl_entry", before_entries)):
        after = staging_mtimes(daily.staging_dir, model)
        for period, when in before.items():
            if period != "2026-06":
                assert after[period] == when, f"{model} {period} was rewritten"
    # And the affected one actually was written. Without this the assertions above are
    # satisfied by a run that did nothing at all, which is the failure this test would
    # otherwise be blind to. It is a creation rather than a rewrite when the generated
    # ledger carried no adjustment in that period, so both count.
    after_adjustments = staging_mtimes(daily.staging_dir, "fct_gl_adjustment")
    assert "2026-06" in after_adjustments, "the affected period was not written"
    assert after_adjustments["2026-06"] != before_facts.get("2026-06"), (
        "the affected period was not rebuilt")

    # The balance partitions: that period and the ones whose windows read through it,
    # computed from the rule rather than hard-coded.
    closure = balances.dirty_closure({"2026-06"}, last_period="2026-12")
    assert len(closure) > 1, "the closure should reach past the dirty period itself"
    after_balances = staging_mtimes(daily.staging_dir, "agg_monthly_balance")
    for period, when in before_balances.items():
        if period not in closure:
            assert after_balances[period] == when, (
                f"agg_monthly_balance {period} is outside the closure and was rewritten")
    assert after_balances["2026-06"] != before_balances["2026-06"]


def test_a_run_that_died_says_which_step_it_died_in(db, daily, monkeypatch, capsys):
    """Case 31. The criterion this task was opened for.

    Nothing is asserted about the mart. That assertion would be true - `recompute` runs
    before `mart-load`, so nothing had written to it yet - but it would be true because
    of the ordering rather than because failure is handled well, and a reader would take
    it for a general property. There is no general property to take: docs/adr/0034 says
    a failed `dbt build` leaves the rows it wrote in the mart until the next successful
    build, which is what `swap-the-mart-into-place` exists for.
    """
    from transform import backfill

    def explode(*args, **kwargs):
        raise RuntimeError("no such period 2026-13")

    monkeypatch.setattr(backfill, "run", explode)

    with pytest.raises(RuntimeError):
        runner.run_pipeline(daily, command="daily", steps=step_list.DAILY)

    record = runs.RunLog(daily.raw_dir).read()[-1]
    assert record.status == "failed"
    assert record.failed_step == "recompute"
    assert "no such period 2026-13" in record.error
    assert [step.status for step in record.steps
            if step.step in ("validate", "load")] == ["succeeded", "succeeded"]
    assert detail_of(record, "load")["tables"], "the load recorded nothing it did"

    assert runs.main(["--raw", str(daily.raw_dir), "--run", record.run_id]) == 0
    printed = capsys.readouterr().out
    assert "recompute" in printed and "no such period 2026-13" in printed


def test_a_failed_dbt_build_leaves_the_periods_owed(db, daily, monkeypatch):
    """Case 32. The step whose failure matters most: it is the one after which the work
    would be lost if `clear-affected` ran too early. The next run over an unchanged
    source has to rebuild those same periods rather than seeing nothing owed."""
    from pipeline import dbt

    monkeypatch.setattr(dbt, "build", lambda *a, **k: (_ for _ in ()).throw(
        RuntimeError("dbt build failed")))

    with pytest.raises(RuntimeError):
        runner.run_pipeline(daily, command="daily", steps=step_list.DAILY)

    record = runs.RunLog(daily.raw_dir).read()[-1]
    assert record.failed_step == "dbt-build"

    owed = affected.read(daily.raw_dir)
    assert not owed.is_empty(), "the affected-period set was cleared before the mart"

    monkeypatch.undo()
    runner.run_pipeline(daily, command="daily", steps=step_list.DAILY)
    rebuilt = detail_of(runs.RunLog(daily.raw_dir).read()[-1], "recompute")["periods"]
    assert set(rebuilt) >= set(owed.periods)


def test_a_killed_run_is_reported_as_interrupted(daily):
    """Case 33. No `finished` event at all, which is what a process that was killed
    leaves. Reporting it as interrupted rather than guessing is docs/adr/0019's
    discipline; naming the step is what docs/adr/0044 adds to it."""
    run_id = runner.open_run(daily, command="daily", steps=step_list.DAILY)
    runner.run_step(daily, run_id, step_list.DAILY[0])
    log = runs.RunLog(daily.raw_dir)
    log.step_started(run_id, "load")

    record = log.read()[-1]

    assert record.status == runs.INTERRUPTED
    assert record.unfinished_step == "load"


def append_source_row(source_dir, contract, row):
    """One more line on a source CSV, as the ERP's next export would carry it."""
    import csv

    path = source_dir / f"{contract['table']}.csv"
    columns = [column["name"] for column in contract["columns"]]
    with path.open("a", newline="", encoding="utf-8") as handle:
        csv.DictWriter(handle, fieldnames=columns).writerow(
            {name: row.get(name, "") for name in columns})


# docs/adr/0036: five previous builds is what the drift gate needs before it will fire,
# and dbt_project.yml is where the number lives.
DRIFT_WINDOW = 5


def seed_the_baseline(db, mart_schema: str, model: str, count: int) -> None:
    """A history the next build's real count cannot match.

    The drift gate is the one gate that can be turned red without touching the ledger,
    which is what this needs: `mart-load` rebuilds the landing schema from the staging
    Parquet on every run, so a failure planted in landing would be gone before dbt saw
    it. Seeded into the promoted mart, the history is carried into the build schema by
    `prepare` and the gate reads it there.
    """
    with db.cursor() as cursor:
        for index in range(DRIFT_WINDOW):
            cursor.execute(
                f'INSERT INTO "{mart_schema}".model_row_count '
                "(invocation_id, built_at, model, row_count) "
                "VALUES (%s, now(), %s, %s)",
                (f"seeded-{index}", model, count),
            )
    db.commit()


def test_a_red_gate_leaves_the_previous_runs_mart_in_place(db, daily):
    """31. The whole point, through the entry point somebody actually types. The run
    fails, the record says which step and which schema, and an analyst reading the mart
    sees the figures the last successful run left."""
    runner.run_pipeline(daily, command="daily", steps=step_list.DAILY)
    entries = row_count(db, daily.mart_schema, "fct_gl_entry")
    seed_the_baseline(db, daily.mart_schema, "fct_gl_entry", entries * 3)
    before = mart_state(db, daily.mart_schema)
    before_baseline = baseline(db, daily.mart_schema)

    with pytest.raises(Exception):
        runner.run_pipeline(daily, command="daily", steps=step_list.DAILY)

    record = daily.log().read()[-1]
    assert record.status == runs.FAILED
    assert record.failed_step == "dbt-build"
    detail = detail_of(record, "dbt-build")
    assert detail["promoted"] is False
    assert detail["build_schema"] in promote.build_schemas(db, daily.mart_schema)
    assert mart_state(db, daily.mart_schema) == before
    # "The drift gate's baseline is likewise untouched by the failed build" - which the
    # business tables' counts and checksums do not say, because the snapshot is not one
    # of them.
    assert baseline(db, daily.mart_schema) == before_baseline


def test_the_teardown_rule_takes_a_kept_build_schema_with_it(db, daily):
    """32. A build schema is kept on purpose when a gate goes red, so a teardown that
    dropped three names by name would leak one into the developer's database on every
    run of the suite. The fixtures share one rule and this is it under test - asserted
    rather than trusted, because the leak is invisible until somebody counts schemas."""
    from pipeline import dbt as pipeline_dbt

    runner.run_pipeline(daily, command="daily", steps=step_list.DAILY)
    entries = row_count(db, daily.mart_schema, "fct_gl_entry")
    seed_the_baseline(db, daily.mart_schema, "fct_gl_entry", entries * 3)
    with pytest.raises(pipeline_dbt.DbtFailed):
        pipeline_dbt.build_and_promote(
            landing=daily.landing_schema, mart=daily.mart_schema,
            run_id=runs.new_run_id(), connection=db,
        )
    assert promote.build_schemas(db, daily.mart_schema), "nothing was left to leak"

    drop_schemas_under(db, daily.landing_schema, daily.mart_schema)

    assert schemas_under(db, daily.landing_schema, daily.mart_schema) == []
