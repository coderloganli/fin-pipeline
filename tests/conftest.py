"""How the tests reach Postgres, and how the mart is built for them.

The connection is made inside a fixture a test asks for, never at import or collection
time. Tests that do not ask for it keep working while the containers are stopped, which
is what makes `pytest -m "not db"` useful.

The settings themselves live in `transform/db.py`, and are re-exported here rather than
restated: the loader resolves them too, and two copies would drift. See
docs/adr/0004-services-run-in-containers.md.
"""

import os
from pathlib import Path

import pytest

from pipeline import dbt as pipeline_dbt
from transform.db import (  # noqa: F401  - re-exported for the tests that import them
    DEFAULTS,
    START_COMMAND,
    DatabaseUnavailable,
    connect,
    parse_env_file,
    settings,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def db():
    """A connection to Postgres. Only tests that ask for it pay for it.

    Autocommit, because this connection reads alongside processes that write DDL. A
    plain SELECT opens an implicit transaction in psycopg, and a connection sitting
    idle in one holds a lock on what it read - which is enough to block the loader's
    `DROP TABLE` and the dbt build behind it, indefinitely and with nothing raised.
    Nothing here needs a transaction: the tests read, and the writes they make are
    single statements against schemas of their own.
    """
    values = settings()
    connection = connect(
        host=values["POSTGRES_HOST"],
        port=values["POSTGRES_PORT"],
        dbname=values["POSTGRES_DB"],
        user=values["POSTGRES_USER"],
        password=values["POSTGRES_PASSWORD"],
    )
    connection.autocommit = True
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession, built once for the whole session.

    Local mode, so this is a library with a toolchain requirement rather than a service
    to stand up - see docs/adr/0028-spark-runs-in-process.md, which is where the
    exception to ADR 0004 is argued.

    It fails rather than skips when Spark cannot start, for the reason the database
    fixture does: a skipped test reports success, and a CI run that verified nothing
    comes back green.
    """
    from transform.spark import session as spark_session

    built = spark_session.build("fin-pipeline-tests")
    try:
        yield built
    finally:
        built.stop()


# --- the mart: one pipeline run, many builds --------------------------------
#
# Building the Parquet is slow - the generator, ingest and three Spark jobs - and
# loading it into Postgres is not. So the Parquet is built once per session per
# generator configuration, and every scenario gets its own landing and mart schema
# loaded from it. A scenario that has to plant a failure mutates its own schema and
# nothing else's.
#
# The schemas the suite uses are named from POSTGRES_LANDING_SCHEMA and
# POSTGRES_MART_SCHEMA, which the suite points somewhere of its own: running the tests
# must not overwrite the schemas a developer has been looking at in the same database.
# See docs/adr/0034.

import hashlib
import subprocess
import sys
from dataclasses import dataclass

DBT_PROJECT = REPO_ROOT / "transform" / "dbt"

TEST_LANDING_SCHEMA = "landing_test"
TEST_MART_SCHEMA = "mart_test"

# The whole year, thinly. The range is not a preference: the generator dates its
# dimension moves in July, so a two-month window would contain no dimension with two
# versions - and the point-in-time behaviour the mart has to preserve would go
# unexercised while every test still passed. Forty entries a period keeps a scenario's
# `dbt build` in seconds.
TEST_PERIODS = "2026-01:2026-12"
TEST_ENTRIES_PER_PERIOD = 40


# Postgres truncates an identifier at 63 characters, and dbt appends
# `_dbt_test__audit` - sixteen of them - to the mart schema when `store_failures` is on.
# A name that fits on its own and not with the suffix is worse than one that is too
# long: dbt computes the full name, Postgres stores the truncated one, the relation
# cache misses, and the second build in a schema fails with "relation already exists"
# on a test that is perfectly correct. So the budget is the suffix's, not Postgres's.
AUDIT_SUFFIX = "_dbt_test__audit"
MAX_SCHEMA = 63 - len(AUDIT_SUFFIX)


def schema_for(node_id: str, prefix: str, reserve: int = 0) -> str:
    """A schema name unique to one test, short enough to survive dbt's audit suffix.

    The readable part is kept and the rest is a digest, so a failure names something a
    person can find in psql.

    `reserve` is for a mart schema a build schema will be derived from: docs/adr/0048
    appends `__b<run_id>` to it, and the audit suffix goes on the end of that. A fixture
    that promotes passes the reserve; one that only builds does not, and keeps the
    longer readable name.
    """
    digest = hashlib.sha256(node_id.encode("utf-8")).hexdigest()[:8]
    readable = "".join(c if c.isalnum() else "_" for c in node_id.rsplit("::", 1)[-1])
    budget = MAX_SCHEMA - reserve - len(prefix) - len(digest) - 2
    if budget < 1:
        raise ValueError(
            f"no room for a readable name: prefix {prefix!r} and a reserve of "
            f"{reserve} leave {budget} characters of the {MAX_SCHEMA} available"
        )
    return f"{prefix}_{readable[:budget]}_{digest}".lower()


def like_prefix(name: str) -> str:
    """A LIKE pattern matching this name and anything suffixed to it.

    `_` is a single-character wildcard and every schema name here is full of them, so an
    unescaped prefix would match schemas belonging to other tests.
    """
    escaped = name.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%")
    return escaped + "%"


def schemas_under(connection, *prefixes: str) -> list[str]:
    """Every schema whose name starts with one of these, sorted."""
    clause = " OR ".join(["nspname LIKE %s ESCAPE '\\'"] * len(prefixes))
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT nspname FROM pg_namespace WHERE {clause}",
            tuple(like_prefix(one) for one in prefixes),
        )
        return sorted(row[0] for row in cursor.fetchall())


def drop_schemas_under(connection, *prefixes: str) -> None:
    """The teardown rule both mart fixtures use.

    By prefix rather than by name, because docs/adr/0048 keeps a failed build's schema
    on purpose - and a teardown naming the landing schema, the mart and its audit schema
    would leave that one in the developer's database on every run of the suite.
    """
    with connection.cursor() as cursor:
        for schema in schemas_under(connection, *prefixes):
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    connection.commit()


@dataclass
class Staging:
    """One generator configuration, built through to Parquet."""

    root: Path
    source: Path
    raw: Path
    staging: Path


def build_staging(spark, root: Path, **config) -> Staging:
    """Generator, ingest, then the three Spark jobs. Everything under one directory."""
    from generator import generate
    from generator.config import Config
    from ingest import load as ingest_load
    from transform.spark import balances, facts, scd2

    source, raw_dir, staging_dir = root / "source", root / "raw", root / "staging"
    settings_ = {
        "seed": 42,
        "out_dir": source,
        "periods": TEST_PERIODS,
        "entries_per_period": TEST_ENTRIES_PER_PERIOD,
    }
    settings_.update(config)
    generate(Config(**settings_))

    ingest_load.load_source(source, raw_dir)
    for table in sorted(scd2.MODELS):
        scd2.build(spark, contracts_load(table), raw_dir, staging_dir)
    # Two facts, not one. An adjustment is a single-sided delta against a voucher that
    # already balanced, so it is a fact of its own rather than a row of the entry fact -
    # see docs/adr/0042.
    for model in sorted(facts.SOURCES):
        facts.build(spark, raw_dir, staging_dir, model=model)
    balances.build(spark, staging_dir, periods=TEST_PERIODS)
    return Staging(root=root, source=source, raw=raw_dir, staging=staging_dir)


def contracts_load(table: str) -> dict:
    from ingest import contracts

    return contracts.load(table)


@pytest.fixture(scope="session")
def clean_staging(spark, tmp_path_factory) -> Staging:
    """A correct ledger. The baseline every gate passes on.

    The two dimension moves are on, and they are not failure modes: docs/architecture.md
    says so in as many words - a cost centre that moved department is a legitimate
    business event, not malformed input. They are what gives the mart a dimension with
    two versions to attribute against, which cases 25 and 26 are about.
    """
    return build_staging(
        spark, tmp_path_factory.mktemp("clean"),
        cost_centre_move=True, account_move=True,
    )


@pytest.fixture(scope="session")
def unbalanced_staging(spark, tmp_path_factory) -> Staging:
    """A ledger whose vouchers do not balance. Gate 3's red scenario."""
    return build_staging(
        spark, tmp_path_factory.mktemp("unbalanced"),
        unbalanced_vouchers=True, cost_centre_move=True, account_move=True,
    )


@dataclass
class Build:
    """One mart, in schemas of its own, and what `dbt build` said about it."""

    landing: str
    mart: str
    staging: Staging
    result: subprocess.CompletedProcess

    @property
    def ok(self) -> bool:
        return self.result.returncode == 0

    @property
    def output(self) -> str:
        return self.result.stdout + self.result.stderr

    def statuses(self) -> dict[str, str]:
        """Every node dbt ran, and what it said. A gate that did not run at all is a
        different failure from a gate that ran and passed, and only this tells them
        apart."""
        import json

        path = DBT_PROJECT / "target" / "run_results.json"
        results = json.loads(path.read_text(encoding="utf-8"))["results"]
        return {r["unique_id"]: r["status"] for r in results}

    def failed_tests(self) -> set[str]:
        """The test nodes dbt reported as failing, by their full unique id.

        Read from run_results.json rather than scraped out of the log: a gate is
        asserted by name, and a substring match against console output would pass on a
        message that merely mentioned the name.

        The whole id, not its last segment. A generic test's id is
        `test.<project>.<name>.<hash>`, so the last segment is the hash - matching
        against it finds nothing, silently, for every gate declared in YAML.
        """
        import json

        path = DBT_PROJECT / "target" / "run_results.json"
        results = json.loads(path.read_text(encoding="utf-8"))["results"]
        return {
            r["unique_id"] for r in results if r["status"] in ("fail", "error")
        }


def dbt_env(landing: str, mart: str) -> dict:
    """The environment dbt reads, with the connection this suite resolved.

    `pipeline.dbt` builds the schema half; the connection half is added here because
    `transform.db.settings` is what the suite already trusts to resolve it, and two
    resolvers would drift.
    """
    values = pipeline_dbt.environment(landing, mart)
    values.update(settings())
    values["POSTGRES_LANDING_SCHEMA"] = landing
    values["POSTGRES_MART_SCHEMA"] = mart
    return values


def run_dbt(args: list[str], landing: str, mart: str) -> subprocess.CompletedProcess:
    """Invoke dbt against this build's own schemas.

    A subprocess rather than dbt's Python entry point, because what CI runs is the
    command, and a gate that only fires through an in-process API is a gate whose
    behaviour in CI is untested. That reasoning now lives in `pipeline/dbt.py`, which
    the pipeline's own `dbt-build` step calls: one statement of how dbt is run here,
    rather than one per caller.
    """
    return subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", *args,
         "--project-dir", str(DBT_PROJECT), "--profiles-dir", str(DBT_PROJECT)],
        env=dbt_env(landing, mart),
        capture_output=True,
        text=True,
    )


@pytest.fixture(scope="session")
def dbt_manifest() -> Path:
    """The manifest, built if it is not on disk.

    `target/` is ignored, so a fresh clone has no manifest and neither does CI - which
    runs the suite before it generates the docs, and has to, because a graph of a
    project whose tests have not run is a graph of something nobody has checked. The
    tests that read the manifest therefore build it rather than assuming it: `dbt parse`
    writes one and does not touch the database.
    """
    target = DBT_PROJECT / "target" / "manifest.json"
    if target.is_file():
        return target

    result = run_dbt(["parse"], TEST_LANDING_SCHEMA, TEST_MART_SCHEMA)
    if not target.is_file():
        raise AssertionError(
            f"`dbt parse` wrote no manifest at {target}:\n"
            f"{result.stdout}\n{result.stderr}"
        )
    return target


@pytest.fixture
def mart(request, db):
    """Load a staging directory into schemas of this test's own, and build.

    `mutate` runs after the load and before the build - that is where a scenario
    plants the failure its gate is supposed to stop.
    """
    from transform import load as mart_load

    landing = schema_for(request.node.nodeid, TEST_LANDING_SCHEMA)
    mart_schema = schema_for(request.node.nodeid, TEST_MART_SCHEMA)

    def build(staging: Staging, mutate=None, dbt_args=None, command="build") -> Build:
        mart_load.load_all(
            staging_dir=staging.staging, raw_dir=staging.raw, schema=landing
        )
        if mutate is not None:
            mutate(db, landing)
        result = run_dbt([command, *(dbt_args or [])], landing, mart_schema)
        return Build(landing=landing, mart=mart_schema, staging=staging, result=result)

    yield build

    drop_schemas_under(db, landing, mart_schema)
