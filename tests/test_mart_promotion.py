"""What a red gate does to the mart, end to end.

`tests/test_mart_gates.py` proves each of the six gates can turn red. This file proves
that turning one red no longer publishes anything: the run builds into a schema of its
own and the mart keeps the last figures that passed.

Cases 18-30 of task.md. See docs/adr/0048.
"""

from types import SimpleNamespace

import pytest

from conftest import AUDIT_SUFFIX, drop_schemas_under, schema_for
from ingest import runs
from pipeline import dbt as pipeline_dbt
from test_mart_load import row_count, table_checksum
from transform import promote

# docs/adr/0038: the provenance columns move on a rerun by design, so a comparison of
# two builds excludes them exactly as docs/adr/0017 excludes ingestion metadata.
PROVENANCE = ("source_first_run_id", "source_last_run_id")

# The seven models `model_row_count` counts. It does not count itself, and it is not one
# of them - but it is in the mart, which is why case 18 expects eight tables.
BUSINESS_MODELS = ("fct_gl_entry", "fct_gl_adjustment", "agg_monthly_balance",
                   "dim_account", "dim_cost_center", "dim_fx_rate", "dim_vendor")

# What a build schema costs a mart schema name: `__b` and a run id.
RESERVE = len(promote.BUILD_INFIX) + len(runs.new_run_id())


def duplicate_a_row(table: str):
    """A uniqueness gate turned red, planted in the landing schema before the build."""
    def mutate(db, schema: str) -> None:
        with db.cursor() as cursor:
            cursor.execute(
                f'INSERT INTO "{schema}".{table} SELECT * FROM "{schema}".{table} LIMIT 1'
            )
        db.commit()
    return mutate


def mart_state(db, schema: str) -> dict:
    return {
        table: (row_count(db, schema, table),
                table_checksum(db, schema, table, exclude=PROVENANCE))
        for table in BUSINESS_MODELS
    }


def tables_in(db, schema: str) -> set[str]:
    with db.cursor() as cursor:
        cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = %s", (schema,))
        return {row[0] for row in cursor.fetchall()}


def invocations(db, schema: str) -> set[str]:
    with db.cursor() as cursor:
        cursor.execute(f'SELECT DISTINCT invocation_id FROM "{schema}".model_row_count')
        return {row[0] for row in cursor.fetchall()}


def baseline(db, schema: str) -> list[tuple]:
    """Every row of the drift gate's memory, not just which builds are in it.

    The requirement says the baseline is untouched by a failed build, and a set of
    invocation ids does not say that: a row whose count changed, or a row that went,
    leaves the set of ids exactly as it was.
    """
    with db.cursor() as cursor:
        cursor.execute(
            f'SELECT invocation_id, built_at, model, row_count '
            f'FROM "{schema}".model_row_count ORDER BY invocation_id, model'
        )
        return cursor.fetchall()


@pytest.fixture
def promoting(request, db, clean_staging):
    """A landing schema loaded from the clean ledger, and a promoting build over it.

    The mart schema reserves room for the build suffix. Without it dbt's audit suffix
    pushes the name past Postgres's 63 characters, which truncates silently and fails a
    second build with `relation already exists` on a test that is perfectly correct.
    """
    from transform import load as mart_load

    landing = schema_for(request.node.nodeid, "lgp")
    mart = schema_for(request.node.nodeid, "mtp", reserve=RESERVE)

    def build(*, mutate=None, args=None, run_id=None, reload=True):
        if reload:
            mart_load.load_all(staging_dir=clean_staging.staging,
                               raw_dir=clean_staging.raw, schema=landing)
        if mutate is not None:
            mutate(db, landing)
        return pipeline_dbt.build_and_promote(
            landing=landing, mart=mart, run_id=run_id or runs.new_run_id(),
            connection=db, args=args,
        )

    yield SimpleNamespace(build=build, landing=landing, mart=mart)

    drop_schemas_under(db, landing, mart)


# On the tests that need Postgres, not on the module: the last three are wiring and
# `pytest -m "not db"` should still check them.
db_test = pytest.mark.db


@db_test
def test_a_green_build_is_promoted_and_leaves_no_schema_behind(promoting, db):
    """18. The seven business models and the drift gate's snapshot, eight tables, in the
    mart schema - and the schema they were built in is gone."""
    run_id = runs.new_run_id()

    detail = promoting.build(run_id=run_id)

    assert tables_in(db, promoting.mart) == set(BUSINESS_MODELS) | {"model_row_count"}
    assert promote.build_schemas(db, promoting.mart) == []
    assert detail["promoted"] is True
    assert detail["build_schema"] == promote.build_schema(promoting.mart, run_id)
    assert detail["swept"] == []


@db_test
def test_a_red_gate_leaves_the_mart_exactly_as_it_was(promoting, db):
    """19. The acceptance criterion. Every table's row count and checksum is what the
    last successful build left, because the failing figures were never in this schema."""
    promoting.build()
    before = mart_state(db, promoting.mart)

    with pytest.raises(pipeline_dbt.DbtFailed):
        promoting.build(mutate=duplicate_a_row("fct_gl_entry"))

    assert mart_state(db, promoting.mart) == before


@db_test
def test_a_red_gate_leaves_its_build_schema_and_names_it(promoting, db):
    """20. `pipeline/run.py` reads `step_detail` off a failure and records it, so the
    schema an operator needs is in the run record with no second mechanism."""
    promoting.build()

    with pytest.raises(pipeline_dbt.DbtFailed) as raised:
        promoting.build(mutate=duplicate_a_row("fct_gl_entry"))

    detail = raised.value.step_detail
    assert detail["promoted"] is False
    assert detail["build_schema"] in promote.build_schemas(db, promoting.mart)


@db_test
def test_a_red_gate_leaves_its_failing_rows_readable(promoting, db):
    """21. `store_failures` is on project-wide precisely so a gate can say what it
    caught. Dropping the schema on failure would throw that away at the moment somebody
    wants it."""
    promoting.build()

    with pytest.raises(pipeline_dbt.DbtFailed) as raised:
        promoting.build(mutate=duplicate_a_row("fct_gl_entry"))

    audit = raised.value.step_detail["build_schema"] + AUDIT_SUFFIX
    stored = [table for table in tables_in(db, audit) if "fct_gl_entry" in table]
    assert stored, f"no stored failures in {audit}; it holds {tables_in(db, audit)}"


@db_test
def test_a_red_gate_does_not_enter_the_drift_baseline(promoting, db):
    """22. `model_row_count` is appended to before the drift gate runs, so a failed build
    has already written its row. It goes away with the schema it was discarded in, which
    is what makes the baseline a history of builds that were accepted."""
    promoting.build()
    before = baseline(db, promoting.mart)

    with pytest.raises(pipeline_dbt.DbtFailed):
        promoting.build(mutate=duplicate_a_row("fct_gl_entry"))

    assert baseline(db, promoting.mart) == before


@db_test
def test_the_baseline_survives_the_swap(promoting, db):
    """23. The snapshot is the one thing in the mart that cannot be recomputed from its
    inputs. A promotion that lost it would leave the drift gate silent on exactly the
    run where somebody wanted it."""
    first = promoting.build()
    after_first = baseline(db, promoting.mart)
    assert len(invocations(db, promoting.mart)) == 1

    promoting.build()

    after_second = baseline(db, promoting.mart)
    # Every row the first build wrote is still there, unchanged, with the second's added
    # beside them. A count of distinct builds would pass on a snapshot that had been
    # rewritten. As a subset rather than a prefix: `invocation_id` is a UUID, so ordering
    # by it says nothing about which build came first.
    assert set(after_first) <= set(after_second)
    assert len(after_second) == 2 * len(after_first)
    assert len(invocations(db, promoting.mart)) == 2
    assert first["build_schema"] not in promote.build_schemas(db, promoting.mart)


@db_test
def test_a_green_build_after_a_red_one_replaces_the_mart(promoting, db):
    """24. The failure is not sticky. The mart holds the green build's figures, and the
    red one contributed nothing to the baseline it will be measured against."""
    promoting.build()
    with pytest.raises(pipeline_dbt.DbtFailed) as raised:
        promoting.build(mutate=duplicate_a_row("fct_gl_entry"))
    red = raised.value.step_detail["build_schema"]

    promoting.build()

    assert row_count(db, promoting.mart, "fct_gl_entry") == row_count(
        db, promoting.landing, "fct_gl_entry")
    assert len(invocations(db, promoting.mart)) == 2
    assert red not in promote.build_schemas(db, promoting.mart)


@db_test
def test_two_reds_in_a_row_both_survive(promoting, db):
    """25. The sweep runs on success, so a discard is not destroyed by the next attempt
    at the thing that failed - which is usually minutes later and before anyone has
    looked at it."""
    promoting.build()

    discarded = []
    for _ in range(2):
        with pytest.raises(pipeline_dbt.DbtFailed) as raised:
            promoting.build(mutate=duplicate_a_row("fct_gl_entry"))
        discarded.append(raised.value.step_detail["build_schema"])

    assert len(set(discarded)) == 2
    assert sorted(promote.build_schemas(db, promoting.mart)) == sorted(discarded)


@db_test
def test_the_green_build_that_follows_sweeps_them_both(promoting, db):
    """26. Bounded by how long a broken pipeline is left broken, and the sweep says in
    its return value which schemas it took."""
    promoting.build()
    discarded = []
    for _ in range(2):
        with pytest.raises(pipeline_dbt.DbtFailed) as raised:
            promoting.build(mutate=duplicate_a_row("fct_gl_entry"))
        discarded.append(raised.value.step_detail["build_schema"])

    detail = promoting.build()

    assert sorted(detail["swept"]) == sorted(discarded)
    assert promote.build_schemas(db, promoting.mart) == []


@db_test
def test_full_refresh_does_not_drop_the_baseline(promoting, db):
    """27. `full_refresh=false` on the model and the copy `prepare` makes are both still
    in force, so the flag an operator reaches for when something looks wrong does not
    silence the gate that would tell them what."""
    promoting.build()
    promoting.build()
    before = invocations(db, promoting.mart)

    promoting.build(args=["--full-refresh"])

    assert before < invocations(db, promoting.mart)
    assert len(invocations(db, promoting.mart)) == 3


# --- wiring -----------------------------------------------------------------

def test_the_dbt_environment_carries_the_resolved_connection(tmp_path, monkeypatch):
    """28. `profiles.yml` falls back to its own defaults for anything the environment
    does not carry, so a database named only in `.env` reached the loader and not dbt.
    With a promotion that means dropping the mart of one database having built in
    another."""
    from transform import db as transform_db

    (tmp_path / ".env").write_text("POSTGRES_DB=named_only_in_dotenv\n", encoding="utf-8")
    monkeypatch.delenv("POSTGRES_DB", raising=False)
    monkeypatch.setattr(transform_db, "REPO_ROOT", tmp_path)

    values = pipeline_dbt.environment("some_landing", "some_mart")

    assert values["POSTGRES_DB"] == "named_only_in_dotenv"
    assert values["POSTGRES_LANDING_SCHEMA"] == "some_landing"
    assert values["POSTGRES_MART_SCHEMA"] == "some_mart"


def test_the_step_refuses_a_context_with_no_run_id():
    """29. The build schema is named after the run. A step that invented one would build
    into a schema no record mentions, which is the opposite of what this is for."""
    from pipeline import run as runner
    from pipeline import steps as step_list

    context = runner.Context(landing_schema="landing_x", mart_schema="mart_x")
    assert context.run_id is None

    with pytest.raises(Exception) as raised:
        step_list.DBT_BUILD.run(context)

    assert "run_id" in str(raised.value)


def test_the_step_passes_the_runs_own_identifier_through(monkeypatch):
    """30. The schema the step builds into is the one `build_schema` derives from the
    run the record opened, so the two can be read against each other."""
    from pipeline import run as runner
    from pipeline import steps as step_list

    seen = {}

    def fake(*, landing, mart, run_id, connection=None, args=None):
        seen.update(landing=landing, mart=mart, run_id=run_id)
        return {"exit_code": 0}

    monkeypatch.setattr(pipeline_dbt, "build_and_promote", fake)
    context = runner.Context(landing_schema="landing_x", mart_schema="mart_x")
    context.run_id = "20260910T031500Z-abc123"

    step_list.DBT_BUILD.run(context)

    assert seen["run_id"] == context.run_id
    assert seen["mart"] == "mart_x"
    assert seen["landing"] == "landing_x"
