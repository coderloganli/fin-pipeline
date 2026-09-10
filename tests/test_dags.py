"""The DAG files, read rather than run.

`docs/adr/0004` keeps `apache-airflow` out of the host's dependency declaration, and
this repository holds that a test which skips when its subject is absent is not a test.
Both survive because the DAG files hold no logic: the sequencing, the stop-on-failure
rule, the run record and the clearing of the affected-period set live in `pipeline/`,
which the rest of the suite exercises directly. See docs/adr/0046.

So these parse the files with `ast` and import no Airflow. An AST can only read
literals, which is why the DAG files are written to a convention `dags/README.md`
states: every `task_id` is a string literal naming its step, and the close task carries
the literal `trigger_rule=TriggerRule.ALL_DONE`.

What this cannot reach - that the edges connect, that XCom carries the identifier, that
the trigger rule behaves as its name says, that Airflow accepts the file at all - is
reached by `docker compose run --rm airflow-dag-processor airflow dags list`, which is
a command and not a test. docs/adr/0046 records that limit rather than hiding it.

Cases 21-23 of orchestrate-the-daily-run.
"""

import ast
from pathlib import Path

import pytest

from pipeline import steps as step_list

DAGS = Path(__file__).resolve().parent.parent / "dags"

OPEN_RUN = "open-run"
CLOSE_RUN = "close-run"

PIPELINES = {
    "daily.py": step_list.DAILY,
    "backfill.py": step_list.BACKFILL,
}


def tree_of(name):
    path = DAGS / name
    assert path.is_file(), f"no DAG file at {path}"
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def task_ids(tree):
    """Every `task_id="..."` in the file, in the order it is written."""
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if keyword.arg == "task_id" and isinstance(keyword.value, ast.Constant):
                found.append(keyword.value.value)
    return found


def call_with_task_id(tree, wanted):
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if (keyword.arg == "task_id" and isinstance(keyword.value, ast.Constant)
                    and keyword.value.value == wanted):
                return node
    return None


def imported_roots(tree):
    roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            roots.add(node.module.split(".")[0])
    return roots


@pytest.mark.parametrize("name", sorted(PIPELINES))
def test_a_dag_declares_a_task_for_every_step_in_order(name):
    """Case 21. A step added to `pipeline.steps` and not to the DAG, or renamed in one
    and not the other, would otherwise appear as a step that quietly never ran."""
    declared = task_ids(tree_of(name))

    assert declared == [OPEN_RUN] + [step.name for step in PIPELINES[name]] + [CLOSE_RUN]


@pytest.mark.parametrize("name", sorted(PIPELINES))
def test_the_close_task_runs_however_the_run_ended(name):
    """Case 22. A close task that only ran on success would leave every failed run
    recorded as `interrupted` - which is the one reading this whole task exists to make
    precise. The literal is what the AST can see; that the rule behaves as its name
    says is Airflow's to keep, not this test's."""
    call = call_with_task_id(tree_of(name), CLOSE_RUN)
    assert call is not None, f"{name} declares no {CLOSE_RUN} task"

    rules = [ast.unparse(keyword.value) for keyword in call.keywords
             if keyword.arg == "trigger_rule"]
    assert rules == ["TriggerRule.ALL_DONE"]


@pytest.mark.parametrize("name", sorted(PIPELINES))
def test_a_dag_imports_nothing_below_the_runner(name):
    """Case 23. The boundary is the thing worth holding: `ingest`, `transform` or
    `generator` in a DAG file means logic has leaked back into the layer docs/adr/0046
    took it out of."""
    roots = imported_roots(tree_of(name))

    assert "pipeline" in roots
    assert roots.isdisjoint({"ingest", "transform", "generator"})
