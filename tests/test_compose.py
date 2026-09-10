"""Airflow in `compose.yaml`, and the image it runs from.

`docs/adr/0004` says every service after Postgres is added to the same file by the task
that needs it, and that the host edits code and runs tests rather than running
services. `docs/adr/0047` is that task for Airflow: three long-running services on
`LocalExecutor` - so no worker and no broker - plus a one-shot init service, against the
Postgres already declared here.

These read files. Nothing here starts a container, and nothing imports Airflow.

Cases 24-28 of orchestrate-the-daily-run.
"""

import re
import tomllib
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent

SERVICES = ("airflow-apiserver", "airflow-scheduler", "airflow-dag-processor")
INIT = "airflow-init"


def compose() -> dict:
    with (REPO_ROOT / "compose.yaml").open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def pyproject() -> dict:
    with (REPO_ROOT / "pyproject.toml").open("rb") as handle:
        return tomllib.load(handle)


def dockerfile() -> str:
    path = REPO_ROOT / "Dockerfile"
    assert path.is_file(), f"no Dockerfile at {path}"
    return path.read_text(encoding="utf-8")


def default_of(value: str) -> str:
    """`${FOO:-bar}` is `bar`; anything else is itself."""
    return re.sub(r"^\$\{[A-Za-z_][A-Za-z0-9_]*:-(.*)\}$", r"\1", value.strip())


def environment_of(service: dict) -> dict:
    """compose accepts a mapping or a list of `KEY=value`; normalise to a mapping."""
    declared = service.get("environment", {})
    if isinstance(declared, dict):
        return {key: str(value) for key, value in declared.items()}
    pairs = (item.split("=", 1) for item in declared)
    return {key: value for key, value in pairs}


def test_the_base_image_python_satisfies_requires_python():
    """Case 24. The two are edited by different tasks. The unsuffixed `apache/airflow`
    tag carries whatever Python was newest at that release - 3.12 today - and this
    project requires 3.13, so the wrong tag fails at image build. The same guard
    already exists for CI's Python. See docs/adr/0047."""
    tag = re.search(r"FROM\s+apache/airflow:(\S+)", dockerfile())
    assert tag, "the Dockerfile does not build on an apache/airflow image"

    version = re.search(r"-python(\d+)\.(\d+)$", tag.group(1))
    assert version, f"pin a python-suffixed tag, not {tag.group(1)!r}"

    floor = re.search(r"(\d+)\.(\d+)", pyproject()["project"]["requires-python"])
    image = (int(version.group(1)), int(version.group(2)))
    assert image >= (int(floor.group(1)), int(floor.group(2)))


def test_the_airflow_services_are_declared_and_wait_for_the_init():
    """Case 25. LocalExecutor, so the scheduler runs tasks in its own process: no
    worker service, no Redis, no broker to keep alive. Built from this repository's
    Dockerfile rather than a stock image, because a step needs a JDK and dbt."""
    services = compose()["services"]

    assert INIT in services
    for name in SERVICES:
        assert name in services, f"compose.yaml declares no {name}"
        service = services[name]
        assert "build" in service, f"{name} does not build from the Dockerfile"
        assert environment_of(service).get("AIRFLOW__CORE__EXECUTOR") == "LocalExecutor"
        waits = service.get("depends_on", {})
        assert INIT in waits, f"{name} does not wait for {INIT}"
        # Not merely "starts after". The init container creates the database and
        # migrates it, then exits; a service that started when it started would race
        # the migration it depends on.
        assert waits[INIT].get("condition") == "service_completed_successfully", (
            f"{name} does not wait for {INIT} to have completed")


def test_airflow_keeps_its_history_out_of_the_mart_database():
    """Case 26. The mart is dropped and rebuilt routinely. Airflow's history must not
    be inside that blast radius, and a second database is what keeps it out - not a
    second Postgres, which would be an image and a volume for the same answer."""
    services = compose()["services"]
    mart = default_of(environment_of(services["postgres"]).get("POSTGRES_DB", ""))

    named = set()
    for name in (*SERVICES, INIT):
        environment = environment_of(services[name])
        airflow_db = default_of(environment.get("AIRFLOW_DB", ""))
        assert airflow_db, f"{name} does not name the Airflow database"
        assert airflow_db != mart, (
            f"{name} puts Airflow's history in the mart database {mart!r}")
        named.add(airflow_db)

    # One database, not four that happen to differ from the mart: services pointed at
    # different metadata databases would each migrate their own and disagree silently.
    assert len(named) == 1, f"the services name different databases: {sorted(named)}"
    airflow_db = named.pop()

    for name in SERVICES:
        connection = environment_of(services[name]).get(
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "")
        assert connection, f"{name} declares no metadata database"
        assert "@postgres" in connection, (
            f"{name} should use the postgres service already declared here")
        assert default_of(connection.rstrip().rsplit("/", 1)[-1]) == airflow_db, (
            f"{name}'s connection string does not end at {airflow_db!r}")

    # And something has to create it: the Postgres image will not, because it runs
    # /docker-entrypoint-initdb.d/ only against an empty data directory.
    init = " ".join(str(part) for part in services[INIT].get("command", []))
    assert "create_metadata_database" in init, (
        "nothing creates the Airflow database before it is migrated")
    assert "airflow db migrate" in init


def test_a_dag_added_to_the_repository_is_a_dag_the_processor_can_see():
    """Case 27. Without the mounts, editing a DAG or a step would mean rebuilding the
    image, and the file the dag-processor parses would not be the file in the
    repository."""
    services = compose()["services"]

    for name in SERVICES:
        mounted = " ".join(str(volume) for volume in services[name].get("volumes", []))
        for directory in ("dags", "pipeline", "ingest", "transform"):
            assert f"./{directory}:" in mounted, (
                f"{name} does not mount {directory}/, so a change there would need an "
                f"image rebuild before the container saw it")


def test_the_runner_is_a_package_that_installs():
    """Case 28. The packages list is explicit rather than discovered, so a new package
    that is not added to it installs as nothing and fails only where it is imported -
    which, for a package the DAGs import inside a container, is a long way from here."""
    assert "pipeline" in pyproject()["tool"]["setuptools"]["packages"]
