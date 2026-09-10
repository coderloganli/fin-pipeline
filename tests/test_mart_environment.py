"""What the mart needs installed, and what CI has to run.

docs/adr/0003 declared `dbt` as an optional group and left it uninstalled, saying the
task that first needs it is the task that makes it install and settles its floor. This
is that task, so these are the assertions that say so — and they are the same shape as
the existing test that pins the Python version to `pyproject.toml`, because that pair
had already drifted apart once before anything checked.

Cases 72-76 of task.md.
"""

import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = REPO_ROOT / "pyproject.toml"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
README = REPO_ROOT / "README.md"
DBT_PROJECT = REPO_ROOT / "transform" / "dbt"

# docs/adr/0003 set this floor: the release confirmed to support Python 3.13 on the
# Postgres adapter. Moving it is a decision to record there, not an edit here.
DBT_CORE_FLOOR = (1, 10)


def workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def steps() -> list[dict]:
    return workflow()["jobs"]["test"]["steps"]


def run_commands() -> list[str]:
    return [step["run"] for step in steps() if "run" in step]


def test_dbt_is_installed_and_meets_its_floor():
    """72. Imports, and reports a core version at or above the floor docs/adr/0003
    set. A version below it would not run on the interpreter this project targets."""
    from dbt.version import get_installed_version

    installed = get_installed_version()
    version = (int(installed.major), int(installed.minor))

    assert version >= DBT_CORE_FLOOR, f"dbt-core {installed.to_version_string()}"

    import dbt.adapters.postgres  # noqa: F401

    # Through the command line as well, because that is what CI runs and what
    # tests/conftest.py invokes. An importable package with a broken entry point would
    # pass the check above and fail every gate scenario.
    reported = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "--version"],
        capture_output=True, text=True,
    )
    assert reported.returncode == 0, reported.stdout + reported.stderr
    assert installed.to_version_string().lstrip("=") in reported.stdout
    assert "postgres" in reported.stdout


def test_the_dbt_project_parses():
    """73. Against its own `profiles.yml`, with connection settings from the
    environment. A project that does not parse has no manifest, and without a manifest
    the impact list in case 65 has nothing to read."""
    from conftest import run_dbt

    result = run_dbt(["parse"], "landing", "mart")

    assert result.returncode == 0, result.stdout + result.stderr


def test_the_install_line_is_the_same_everywhere():
    """74. `pyproject.toml`, the CI workflow and the README have to agree. They are
    three statements of one fact, and the one that drifts is the one nobody runs."""
    extras = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"][
        "optional-dependencies"
    ]
    assert {"dev", "spark", "dbt"} <= set(extras)

    installs = [line for line in run_commands() if line.strip().startswith("pip install")]
    assert len(installs) == 1
    assert "dbt" in installs[0]

    readme = README.read_text(encoding="utf-8")
    assert installs[0].strip() in readme


def test_ci_generates_the_docs_renders_the_graph_and_uploads_it():
    """75. The artefact is one of this task's three acceptance criteria, and an
    artefact CI does not produce is a criterion that is met on one machine."""
    commands = run_commands()

    assert any("dbt docs generate" in step for step in commands)
    assert any("transform.lineage" in step for step in commands)

    # And after the suite, not before it. Documenting a project whose tests have not run
    # publishes a graph of something nobody has checked.
    order = [i for i, step in enumerate(commands) if "pytest" in step]
    docs = [i for i, step in enumerate(commands) if "dbt docs generate" in step]
    render = [i for i, step in enumerate(commands) if "transform.lineage" in step]
    assert order and docs and render
    assert max(order) < min(docs) < min(render)

    uploads = [step for step in steps() if "upload-artifact" in str(step.get("uses", ""))]
    assert uploads, "the workflow uploads no artefact"
    assert any("lineage.html" in str(step.get("with", {})) for step in uploads)


def test_the_mart_tests_fail_rather_than_skip_without_postgres(monkeypatch):
    """76. They inherit this from `tests/conftest.py` and do not opt out. A skipped
    test reports success, and a CI run that verified nothing comes back green. See
    docs/adr/0004."""
    from transform import db as transform_db

    monkeypatch.setenv("POSTGRES_HOST", "127.0.0.1")
    monkeypatch.setenv("POSTGRES_PORT", "1")

    with pytest.raises(transform_db.DatabaseUnavailable) as failure:
        values = transform_db.settings(repo_root=Path("/nonexistent"))
        transform_db.connect(
            host=values["POSTGRES_HOST"], port=values["POSTGRES_PORT"],
            dbname=values["POSTGRES_DB"], user=values["POSTGRES_USER"],
            password=values["POSTGRES_PASSWORD"],
        )

    assert "docker compose up -d" in str(failure.value)

    source = (Path(__file__).parent / "test_mart_gates.py").read_text(encoding="utf-8")
    assert "skipif" not in source
    assert "pytest.skip" not in source
