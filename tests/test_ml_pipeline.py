"""Where judging sits in a run, and what it is given.

Cases 32-36 of task.md. See docs/adr/0050: the step runs after `dbt-build` has promoted
the mart and before `clear-affected` has cleared what it needs to read, and it works out
its own target periods rather than being handed them by the step before - ADR-0046 keeps
step-to-step handoff out of the DAG.

Case 35 is the one that matters most here. The whole reason the flags live outside the
mart is that this layer must not be able to unpublish it, and the only way to assert
that is to fail the step on purpose and look at what the mart still holds.
"""

import ast
from pathlib import Path

import pytest

from ingest import affected
from pipeline import run as runner
from pipeline import steps as step_list

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGS = REPO_ROOT / "dags"


@pytest.fixture
def raw_dir(tmp_path):
    directory = tmp_path / "raw"
    directory.mkdir(parents=True)
    return directory


@pytest.fixture
def context(tmp_path, raw_dir):
    return runner.Context(
        source_dir=tmp_path / "source",
        raw_dir=raw_dir,
        staging_dir=tmp_path / "staging",
    )


# --- case 32 ----------------------------------------------------------------

def test_judge_runs_after_the_mart_is_published_and_before_the_set_is_cleared():
    """Case 32. Both halves are load-bearing. After `dbt-build`, because it trains on
    the published mart (docs/adr/0051); before `clear-affected`, because on a daily run
    the affected set is how it knows which periods to judge."""
    names = [step.name for step in step_list.DAILY]

    assert "judge" in names
    assert names.index("dbt-build") < names.index("judge") < names.index("clear-affected")


def test_a_backfill_judges_what_it_rebuilt():
    """Case 32, the other pipeline. A backfill that rebuilt a period's figures and left
    last month's flags standing would leave the queue describing figures that are
    gone."""
    names = [step.name for step in step_list.BACKFILL]

    assert "judge" in names
    assert names.index("dbt-build") < names.index("judge") < names.index("clear-affected")


# --- cases 33 and 34 --------------------------------------------------------

@pytest.fixture
def judged_periods(monkeypatch):
    """Capture the periods the step asks `ml.judge` to judge, without judging."""
    from ml import judge

    seen = []

    def capture(**kwargs):
        seen.append(sorted(kwargs["periods"]))
        return judge.Detail(flagged=0, intervals_crossed=0, periods=kwargs["periods"])

    monkeypatch.setattr(judge, "run", capture)
    return seen


def test_a_daily_run_judges_the_affected_periods_and_their_closure(context, raw_dir,
                                                                   judged_periods):
    """Case 33. Not every period: a nightly run that re-judged three years would spend
    the night doing it. The set comes from `ingest.affected`, widened by
    `transform.spark.balances.dirty_closure` - the same function `recompute` used to
    decide what to rewrite, called rather than restated, which is the point
    docs/adr/0040 makes about a window rule written down twice.

    A daily run carries the reporting range like any other, and `force` is what says
    this is not a backfill - the same flag `python -m pipeline` sets from the pipeline
    name and `transform.backfill.run` reads. Branching on the range instead would make
    every nightly run judge the whole of it.
    """
    from transform.spark import balances as balances_module
    from transform.spark.balances import dirty_closure

    context.periods = "2026-01:2026-12"
    context.force = False
    affected.record(raw_dir, periods=["2026-02", "2026-03"])

    expected = sorted(dirty_closure({"2026-02", "2026-03"}, last_period="2026-12"))
    assert len(expected) < 12, "the closure is meant to be narrower than the range"

    # A sentinel rather than the real closure as the oracle. Comparing against
    # `dirty_closure`'s own output would pass just as well if `_judge` had inlined the
    # same arithmetic - and an inlined copy that stopped tracking a widened window is
    # exactly what docs/adr/0040 is about.
    called = []

    def sentinel(periods, *, last_period):
        called.append((sorted(periods), last_period))
        return {"2099-01"}

    balances_module.dirty_closure = sentinel
    try:
        runner.run_step(context, "r-1", step_list.by_name("daily", "judge"))
    finally:
        balances_module.dirty_closure = dirty_closure

    assert called == [(["2026-02", "2026-03"], "2026-12")]
    assert judged_periods == [["2099-01"]]


def test_a_backfill_judges_exactly_the_range_it_was_given(context, judged_periods):
    """Case 34. A backfill is the case where somebody typed the range, so the affected
    set is precisely what does not name it - `transform.backfill` is passed `force` for
    the same reason. See pipeline/steps.py."""
    context.periods = "2026-02:2026-03"
    context.force = True

    runner.run_step(context, "r-2", step_list.by_name("backfill", "judge"))

    assert judged_periods == [["2026-02", "2026-03"]]


@pytest.mark.db
def test_a_backfill_replaces_the_flags_of_the_range_it_rebuilt(db, mart, ml_staging,
                                                               request, context):
    """Case 34's other half. The captured-range test above passes even if `_judge`
    never writes anything, so this one runs the step for real against a built mart and
    looks at what is in the table afterwards."""
    from conftest import TEST_ANOMALY_SCHEMA, drop_schemas_under, flags_in, schema_for

    built = mart(ml_staging)
    assert built.ok, built.output
    anomaly = schema_for(request.node.nodeid, TEST_ANOMALY_SCHEMA)
    context.mart_schema, context.anomaly_schema = built.mart, anomaly
    context.periods, context.force = "2026-05:2026-06", True

    try:
        first = step_list.by_name("backfill", "judge")
        detail = runner.run_step(context, "backfill-one", first)
        assert detail["periods"] == ["2026-05", "2026-06"]
        before = flags_in(db, anomaly)
        assert before, "the backfill flagged nothing, so replacement cannot be shown"

        runner.run_step(context, "backfill-two", first)
        after = flags_in(db, anomaly)

        assert {r["run_id"] for r in after} == {"backfill-two"}, (
            "the second backfill did not replace the first one's flags"
        )
        assert [{k: v for k, v in r.items() if k != "run_id"} for r in after] == \
               [{k: v for k, v in r.items() if k != "run_id"} for r in before]
    finally:
        drop_schemas_under(db, anomaly)


# --- case 35 ----------------------------------------------------------------

@pytest.mark.db
def test_a_failed_judge_leaves_the_published_mart_where_it_is(db, mart, ml_staging,
                                                              raw_dir, context,
                                                              monkeypatch):
    """Case 35. The argument docs/adr/0050 is built on, asserted rather than stated:
    `docs/product.md` says deleting this layer changes no reported number, so a failure
    in it must not change one either.

    `mart.model_row_count` is excluded from the comparison. Every successful build
    appends a row to it by construction (docs/adr/0036), and docs/adr/0038 already
    excludes it from the reproducibility criterion for the same reason. The gate is
    that the reported figures did not move, not that the schema is byte-identical.
    """
    from ml import judge

    built = mart(ml_staging)
    assert built.ok, built.output
    affected.record(raw_dir, periods=["2026-06"])
    before = reported_counts(db, built.mart)

    def explode(**kwargs):
        raise RuntimeError("the model did not converge")

    monkeypatch.setattr(judge, "run", explode)

    with pytest.raises(RuntimeError):
        runner.run_step(context, "r-3", step_list.by_name("daily", "judge"))

    assert reported_counts(db, built.mart) == before
    assert affected.read(raw_dir).periods == ["2026-06"], (
        "a run that died at judge still owes its affected periods"
    )


def reported_counts(db, schema):
    """Every mart table's row count and a checksum over its rows, less the one table
    that is a record rather than a derivation.

    A count alone would pass on a judge failure that changed values without changing
    how many there were, which is the more likely way for a write to go wrong. The
    checksum is over the rows rendered as text and ordered, the same shape
    docs/adr/0017 uses for the raw layer. `model_row_count` is excluded because every
    successful build appends to it by construction - docs/adr/0036 and 0038.
    """
    with db.cursor() as cursor:
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s ORDER BY table_name", (schema,),
        )
        tables = [row[0] for row in cursor.fetchall() if row[0] != "model_row_count"]
        state = {}
        for table in tables:
            cursor.execute(f'SELECT count(*) FROM "{schema}"."{table}"')
            count = cursor.fetchone()[0]
            cursor.execute(
                f'SELECT md5(string_agg(row_text, chr(10) ORDER BY row_text)) '
                f'FROM (SELECT "{table}"::text AS row_text FROM "{schema}"."{table}") s'
            )
            state[table] = (count, cursor.fetchone()[0])
    return state


# --- case 36 ----------------------------------------------------------------

def tree_of(name: str):
    return ast.parse((DAGS / name).read_text(encoding="utf-8"))


def task_ids(tree) -> list[str]:
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for keyword in node.keywords:
                if keyword.arg == "task_id" and isinstance(keyword.value, ast.Constant):
                    found.append(keyword.value.value)
    return found


@pytest.mark.parametrize("name", ["daily.py", "backfill.py"])
def test_both_dags_declare_a_judge_task(name):
    """Case 36. A step that exists in `pipeline/steps.py` and in no DAG is a step that
    never runs at night, which is the only time it matters."""
    assert "judge" in task_ids(tree_of(name)), f"{name} declares no judge task"
