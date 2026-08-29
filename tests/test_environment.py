"""The local development environment: how dependencies are declared, how the
database container is defined, and whether the two agree with each other.

These tests are the acceptance criteria for the environment itself. Only the one
marked `db` needs a running database; the rest read files, so they stay useful
when the containers are stopped.
"""

import re
import tomllib
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent

# Keys that .env.example declares and compose.yaml must agree with.
DATABASE = "POSTGRES_DB"
USER = "POSTGRES_USER"
PASSWORD = "POSTGRES_PASSWORD"
HOST = "POSTGRES_HOST"
PORT = "POSTGRES_PORT"

# The distribution each optional group exists to carry.
EXPECTED_GROUPS = {
    "spark": "pyspark",
    "dbt": "dbt-core",
    "ml": "scikit-learn",
    "app": "streamlit",
}


def read_pyproject() -> dict:
    with (REPO_ROOT / "pyproject.toml").open("rb") as handle:
        return tomllib.load(handle)


def read_ci_workflow() -> dict:
    with (REPO_ROOT / ".github" / "workflows" / "ci.yml").open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def read_env_example() -> dict[str, str]:
    values = {}
    for line in (REPO_ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def read_compose() -> dict:
    with (REPO_ROOT / "compose.yaml").open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def postgres_service(compose: dict) -> dict:
    services = compose.get("services", {})
    assert "postgres" in services, f"compose.yaml declares no postgres service: {sorted(services)}"
    return services["postgres"]


def interpolation_default(value: str) -> str:
    """Pull `fin_pipeline` out of `${POSTGRES_DB:-fin_pipeline}`."""
    match = re.fullmatch(r"\$\{[A-Z_]+:-([^}]*)\}", str(value).strip())
    assert match, f"expected a ${{VAR:-default}} interpolation, got {value!r}"
    return match.group(1)


def distribution_name(requirement: str) -> str:
    """Reduce `psycopg[binary]` or `dbt-core>=1.10` to its distribution name,
    normalised per PEP 503. Prefix matching would let `psycopg2` stand in for
    `psycopg`, or `pytest-cov` for `pytest`."""
    name = re.split(r"[\[<>=!~;\s]", str(requirement).strip(), maxsplit=1)[0]
    return name.strip().lower().replace("_", "-")


def minor_version(specifier: str) -> str:
    """Reduce `>=3.13` or `3.13.11` to `3.13`."""
    match = re.search(r"(\d+)\.(\d+)", str(specifier))
    assert match, f"no major.minor version found in {specifier!r}"
    return f"{match.group(1)}.{match.group(2)}"


# --- Case 1 - needs a running database -------------------------------------

@pytest.mark.db
def test_the_database_answers_a_query(db):
    with db.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1


# --- Case 2 - no database needed -------------------------------------------

def test_a_missing_database_names_the_command_that_starts_it():
    from conftest import DatabaseUnavailable, connect

    with pytest.raises(DatabaseUnavailable) as failure:
        connect(host="127.0.0.1", port=1, dbname="nope", user="nope", password="nope")

    assert "docker compose up -d" in str(failure.value), (
        "a developer who has not started the containers should be told how to, "
        f"not handed a driver error: {failure.value}"
    )


# --- Case 3 - no database needed -------------------------------------------

def test_the_python_version_does_not_drift_between_pyproject_and_ci():
    declared = read_pyproject()["project"]["requires-python"]

    versions = set()
    for job in read_ci_workflow()["jobs"].values():
        for step in job.get("steps", []):
            if "python-version" in step.get("with", {}):
                versions.add(str(step["with"]["python-version"]))

    assert versions, "no step in the CI workflow sets a python-version"

    for version in versions:
        assert minor_version(version) == minor_version(declared), (
            f"CI runs on {version} while pyproject.toml requires {declared}"
        )


# --- Case 4 - no database needed -------------------------------------------

def test_every_optional_group_is_declared_and_carries_its_package():
    groups = read_pyproject()["project"]["optional-dependencies"]

    missing = sorted(set(EXPECTED_GROUPS) - set(groups))
    assert not missing, f"optional groups not declared: {missing}"

    for group, package in EXPECTED_GROUPS.items():
        requirements = groups[group]
        assert requirements, f"optional group {group!r} is declared but empty"
        names = {distribution_name(requirement) for requirement in requirements}
        assert package in names, (
            f"optional group {group!r} does not carry {package!r}: {sorted(names)}"
        )


# --- Case 5 - no database needed -------------------------------------------

def test_compose_and_env_example_describe_the_same_database():
    env = read_env_example()
    service = postgres_service(read_compose())
    environment = service.get("environment", {})

    for key in (DATABASE, USER, PASSWORD):
        assert key in env, f".env.example does not declare {key}"
        assert key in environment, f"the postgres service does not set {key}"
        assert interpolation_default(environment[key]) == env[key], (
            f"{key} differs: .env.example says {env[key]!r}, "
            f"compose.yaml defaults to {interpolation_default(environment[key])!r}"
        )

    mappings = [str(mapping) for mapping in service.get("ports", [])]
    assert mappings, "the postgres service publishes no ports"

    resolved = []
    for mapping in mappings:
        host_side, _, container_side = mapping.rpartition(":")
        assert container_side == "5432", (
            f"the container side of {mapping!r} should be Postgres's own port 5432"
        )
        resolved.append(
            interpolation_default(host_side) if host_side.startswith("${") else host_side
        )

    assert env[PORT] in resolved, (
        f".env.example connects on port {env[PORT]} but compose publishes {resolved}"
    )

    assert env[HOST] in {"localhost", "127.0.0.1"}, (
        f"compose publishes to the host machine, so {HOST} should be a loopback "
        f"address, not {env[HOST]!r}"
    )


# --- Added at stage 8 - no database needed ---------------------------------

def test_core_and_dev_dependencies_are_declared():
    """Case 4 covered the optional groups. Nothing covered the two groups that
    must actually install, so they could have been emptied unnoticed in an
    environment where the packages happened to be present already."""
    project = read_pyproject()["project"]

    core = {distribution_name(r) for r in project["dependencies"]}
    for package in ("psycopg", "pyyaml"):
        # pyyaml moved here when the source contracts landed: they are read at ingest
        # time, not only under test.
        assert package in core, f"core dependencies do not carry {package!r}: {sorted(core)}"

    dev = {distribution_name(r) for r in project["optional-dependencies"]["dev"]}
    assert "pytest" in dev, f"the dev group does not carry pytest: {sorted(dev)}"


def test_ci_installs_the_same_way_the_readme_says_to():
    """Success criterion 3: CI uses the same install line as a developer does."""
    command = 'pip install -e ".[dev]"'

    steps = [
        step.get("run", "")
        for job in read_ci_workflow()["jobs"].values()
        for step in job.get("steps", [])
    ]
    assert any(command in step for step in steps), (
        f"no CI step runs {command!r}; steps are {[s for s in steps if s]}"
    )

    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert command in readme, f"README.md does not tell a developer to run {command!r}"


def test_settings_prefer_the_environment_over_the_defaults(monkeypatch):
    """The override path is what makes .env and exported variables usable."""
    from conftest import DEFAULTS, settings

    monkeypatch.setenv("POSTGRES_DB", "somewhere_else")
    assert settings()["POSTGRES_DB"] == "somewhere_else"

    monkeypatch.delenv("POSTGRES_DB")
    assert settings()["POSTGRES_DB"] == DEFAULTS["POSTGRES_DB"]


def test_settings_fall_back_to_the_dotenv_file(monkeypatch, tmp_path):
    """Compose reads .env; pytest has to read it too, or editing it moves the
    database the container publishes while the tests keep connecting to the old
    one. Written against a temporary directory so a developer's real .env is
    neither read nor overwritten."""
    import conftest

    (tmp_path / ".env").write_text("POSTGRES_DB=from_dotenv\n", encoding="utf-8")
    monkeypatch.setattr(conftest, "REPO_ROOT", tmp_path)
    monkeypatch.delenv("POSTGRES_DB", raising=False)

    assert conftest.settings()["POSTGRES_DB"] == "from_dotenv"

    # A real environment variable still wins, the way Compose resolves it.
    monkeypatch.setenv("POSTGRES_DB", "from_environment")
    assert conftest.settings()["POSTGRES_DB"] == "from_environment"
