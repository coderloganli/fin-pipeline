"""The promotion boundary: a schema of the run's own, renamed into place.

dbt builds the models and then runs the gates over them, so a gate that goes red does so
after the table it guards has been written. These are the primitives that make that
stop mattering - the mart a reader sees is promoted rather than accumulated.

Cases 1-17 of task.md. See docs/adr/0048.
"""

import time
from types import SimpleNamespace

import pytest

from conftest import AUDIT_SUFFIX, settings
from transform import db as transform_db
from transform import promote

# The marker goes on the tests that need Postgres, not on the module: the name and its
# budget are arithmetic, and `pytest -m "not db"` should still check them.
db_test = pytest.mark.db

MART = "promote_t"
OTHER = "promote_o"
RUN = "20260910T031500Z-abc123"
LATER = "20260910T031501Z-def456"
LATEST = "20260910T031502Z-ghi789"
EARLIER = "20260909T221500Z-jkl012"

ROW_COUNT_COLUMNS = ("invocation_id", "built_at", "model", "row_count")


# --- helpers ----------------------------------------------------------------

def schemas(db, prefix: str) -> set[str]:
    with db.cursor() as cursor:
        cursor.execute(
            "SELECT nspname FROM pg_namespace WHERE nspname LIKE %s", (prefix + "%",)
        )
        return {row[0] for row in cursor.fetchall()}


def make_schema(db, name: str, *tables: str) -> None:
    with db.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA "{name}"')
        for table in tables:
            cursor.execute(f'CREATE TABLE "{name}"."{table}" (marker text)')
            cursor.execute(f'INSERT INTO "{name}"."{table}" VALUES (%s)', (name,))


def make_row_count(db, schema: str, rows) -> None:
    with db.cursor() as cursor:
        cursor.execute(
            f'CREATE TABLE "{schema}".model_row_count ('
            "invocation_id text, built_at timestamptz, model text, row_count bigint)"
        )
        for invocation, model, count in rows:
            cursor.execute(
                f'INSERT INTO "{schema}".model_row_count '
                "(invocation_id, built_at, model, row_count) VALUES (%s, now(), %s, %s)",
                (invocation, model, count),
            )


def tables_in(db, schema: str) -> set[str]:
    with db.cursor() as cursor:
        cursor.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = %s", (schema,)
        )
        return {row[0] for row in cursor.fetchall()}


def columns_of(db, schema: str, table: str) -> list[str]:
    with db.cursor() as cursor:
        cursor.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema, table),
        )
        return [row[0] for row in cursor.fetchall()]


def row_counts(db, schema: str) -> list[tuple]:
    with db.cursor() as cursor:
        cursor.execute(
            f'SELECT invocation_id, model, row_count FROM "{schema}".model_row_count '
            "ORDER BY invocation_id, model"
        )
        return cursor.fetchall()


def marker(db, schema: str, table: str) -> str:
    with db.cursor() as cursor:
        cursor.execute(f'SELECT marker FROM "{schema}"."{table}"')
        return cursor.fetchone()[0]


@pytest.fixture
def clean(db):
    """Every schema this module makes, gone afterwards however the test ended."""
    yield
    with db.cursor() as cursor:
        for prefix in (MART, OTHER, "martxy"):
            cursor.execute(
                "SELECT nspname FROM pg_namespace WHERE nspname LIKE %s",
                (prefix + "%",),
            )
            for (name,) in cursor.fetchall():
                cursor.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
    db.commit()


# --- the name, and its budget ----------------------------------------------

def test_the_build_schema_carries_the_run_id_verbatim():
    """1. The schema name is what somebody pastes into `python -m ingest.runs --run`,
    which it can only be if the identifier went in unchanged."""
    assert promote.build_schema("mart", RUN) == f"mart__b{RUN}"


@db_test
def test_a_build_schemas_own_audit_schema_is_not_a_build_schema(db, clean):
    """2. `<build>_dbt_test__audit` shares the build prefix. Returning it as a build
    schema would have the sweep drop it twice and `promote` reach for a schema that had
    already gone. Nor may the underscores in the prefix act as LIKE wildcards: `martxy`
    is not `mart`."""
    build = promote.build_schema(MART, RUN)
    for name in (MART, build, build + AUDIT_SUFFIX, MART + AUDIT_SUFFIX,
                 "martxybogus"):
        make_schema(db, name)
    db.commit()

    assert promote.build_schemas(db, MART) == [build]


def test_a_mart_schema_at_the_budget_is_accepted():
    """3. The boundary is where the module says it is, not one either side of it."""
    name = "m" * promote.MART_SCHEMA_BUDGET
    built = promote.build_schema(name, RUN)

    assert len(built + AUDIT_SUFFIX) == promote.MAX_IDENTIFIER


def test_a_mart_schema_one_over_the_budget_is_refused():
    """4. Postgres truncates at 63 and dbt appends sixteen; the failure that shape
    produces is `relation already exists` on a test that is perfectly correct, which
    tests/conftest.py had already been bitten by once."""
    name = "m" * (promote.MART_SCHEMA_BUDGET + 1)

    with pytest.raises(promote.SchemaNameTooLong) as raised:
        promote.build_schema(name, RUN)

    message = str(raised.value)
    assert str(len(name)) in message
    assert str(promote.MART_SCHEMA_BUDGET) in message


# --- prepare ----------------------------------------------------------------

@db_test
def test_prepare_creates_an_empty_build_schema_when_there_is_no_mart(db, clean):
    """5. The first build a database ever sees has nothing to carry forward."""
    build = promote.build_schema(MART, RUN)

    promote.prepare(db, MART, build)

    assert build in schemas(db, MART)
    assert tables_in(db, build) == set()


@db_test
def test_prepare_copies_the_row_count_snapshot_forward(db, clean):
    """6. The drift gate's memory is the one thing in the mart that cannot be recomputed
    from its inputs, so it is carried into the schema that will replace the mart."""
    make_schema(db, MART)
    make_row_count(db, MART, [("first", "fct_gl_entry", 100),
                              ("second", "fct_gl_entry", 101)])
    db.commit()
    build = promote.build_schema(MART, RUN)

    promote.prepare(db, MART, build)

    assert row_counts(db, build) == row_counts(db, MART)
    assert columns_of(db, build, "model_row_count") == list(ROW_COUNT_COLUMNS)


@db_test
def test_prepare_copies_nothing_but_the_snapshot(db, clean):
    """7. Every other model is a function of its inputs and dbt rebuilds it. Copying one
    forward would put a table in the build schema that this build did not produce."""
    make_schema(db, MART, "fct_gl_entry")
    make_row_count(db, MART, [("first", "fct_gl_entry", 100)])
    db.commit()
    build = promote.build_schema(MART, RUN)

    promote.prepare(db, MART, build)

    assert tables_in(db, build) == {"model_row_count"}


@db_test
def test_prepare_makes_a_retry_a_fresh_attempt(db, clean):
    """8. An Airflow task that retries carries the same run id and therefore the same
    build schema. Appending to what the failed attempt left would double every count."""
    make_schema(db, MART)
    make_row_count(db, MART, [("first", "fct_gl_entry", 100)])
    db.commit()
    build = promote.build_schema(MART, RUN)

    promote.prepare(db, MART, build)
    with db.cursor() as cursor:
        cursor.execute(
            f'INSERT INTO "{build}".model_row_count '
            "(invocation_id, built_at, model, row_count) VALUES ('attempt', now(), 'x', 1)"
        )
        cursor.execute(f'CREATE TABLE "{build}".half_built (marker text)')
    db.commit()

    promote.prepare(db, MART, build)

    assert row_counts(db, build) == row_counts(db, MART)
    assert tables_in(db, build) == {"model_row_count"}


# --- sweep ------------------------------------------------------------------

@db_test
def test_sweep_drops_every_build_schema_but_the_one_it_keeps(db, clean):
    """9. It runs after a promotion, so there is no ordering to get right: everything
    that is not the schema just promoted is a discard whose problem has been fixed."""
    kept = promote.build_schema(MART, LATEST)
    discarded = [promote.build_schema(MART, run) for run in (RUN, LATER, EARLIER)]
    for name in [kept, *discarded]:
        make_schema(db, name)
    db.commit()

    dropped = promote.sweep(db, MART, keep=kept)

    assert sorted(dropped) == sorted(discarded)
    assert promote.build_schemas(db, MART) == [kept]


@db_test
def test_sweep_takes_a_discards_audit_schema_with_it(db, clean):
    """10. The audit schema is where the failing rows are. Leaving it would leave the
    half of a discard that has rows in it."""
    kept = promote.build_schema(MART, LATEST)
    discarded = promote.build_schema(MART, RUN)
    for name in (kept, discarded, discarded + AUDIT_SUFFIX):
        make_schema(db, name)
    db.commit()

    promote.sweep(db, MART, keep=kept)

    assert discarded + AUDIT_SUFFIX not in schemas(db, MART)


@db_test
def test_sweep_touches_nothing_that_is_not_this_marts_build_schema(db, clean):
    """11. The mart itself, its audit schema, the landing schema, another mart's builds,
    and a schema whose name merely matches the prefix if the underscores are read as
    wildcards."""
    kept = promote.build_schema(MART, LATEST)
    others = [MART, MART + AUDIT_SUFFIX, OTHER,
              promote.build_schema(OTHER, RUN), "martxybogus"]
    for name in [kept, *others]:
        make_schema(db, name)
    db.commit()

    promote.sweep(db, MART, keep=kept)

    for name in others:
        assert name in schemas(db, name), f"{name} was swept and should not have been"


@db_test
def test_sweep_with_nothing_to_drop_says_so(db, clean):
    """12. The ordinary night. A sweep that invented work would be a sweep nobody could
    read the return value of."""
    kept = promote.build_schema(MART, LATEST)
    make_schema(db, kept)
    db.commit()

    assert promote.sweep(db, MART, keep=kept) == []


# --- promote ----------------------------------------------------------------

@db_test
def test_promote_replaces_the_mart_with_the_build_schema(db, clean):
    """13. One rename, not a copy: a copy would replace the tables one at a time, which
    is the failure being fixed rather than a smaller version of it."""
    make_schema(db, MART, "fct_gl_entry")
    build = promote.build_schema(MART, RUN)
    make_schema(db, build, "fct_gl_entry")
    db.commit()

    promote.promote(db, MART, build)

    assert marker(db, MART, "fct_gl_entry") == build
    assert build not in schemas(db, MART)


@db_test
def test_promote_creates_the_mart_when_there_is_none(db, clean):
    """14. The first build in a fresh database has no mart to drop, and refusing there
    would mean a database that can never get its first one."""
    build = promote.build_schema(MART, RUN)
    make_schema(db, build, "fct_gl_entry")
    db.commit()

    promote.promote(db, MART, build)

    assert marker(db, MART, "fct_gl_entry") == build


@db_test
def test_promote_moves_the_audit_schema_with_it(db, clean):
    """15. `store_failures` writes there, and a mart whose audit schema belonged to some
    earlier build would answer the question `what failed` with the wrong build."""
    make_schema(db, MART, "fct_gl_entry")
    make_schema(db, MART + AUDIT_SUFFIX, "unique_fct_gl_entry")
    build = promote.build_schema(MART, RUN)
    make_schema(db, build, "fct_gl_entry")
    make_schema(db, build + AUDIT_SUFFIX, "unique_fct_gl_entry")
    db.commit()

    promote.promote(db, MART, build)

    assert marker(db, MART + AUDIT_SUFFIX, "unique_fct_gl_entry") == build + AUDIT_SUFFIX
    assert build + AUDIT_SUFFIX not in schemas(db, MART)


@db_test
def test_promote_refuses_when_there_is_nothing_to_promote(db, clean):
    """16. The drop must never be the only half that happens. A build schema that is not
    there is a caller that has lost track of its own run, and dropping the mart for it
    would destroy the figures the promotion exists to protect."""
    make_schema(db, MART, "fct_gl_entry")
    db.commit()

    with pytest.raises(promote.NothingToPromote):
        promote.promote(db, MART, promote.build_schema(MART, RUN))

    assert marker(db, MART, "fct_gl_entry") == MART


@db_test
def test_promote_fails_rather_than_waiting_forever_on_a_lock(db, clean, monkeypatch):
    """17. tests/conftest.py records what happens without a lock timeout: a connection
    idle in a transaction holds a lock on what it read, which blocks DDL indefinitely
    and with nothing raised. A promotion that hangs is worse than one that fails,
    because the run never reports at all."""
    make_schema(db, MART, "fct_gl_entry")
    build = promote.build_schema(MART, RUN)
    make_schema(db, build, "fct_gl_entry")
    db.commit()

    monkeypatch.setattr(promote, "LOCK_TIMEOUT", "1s")
    values = settings()
    blocker = transform_db.connect(
        host=values["POSTGRES_HOST"], port=values["POSTGRES_PORT"],
        dbname=values["POSTGRES_DB"], user=values["POSTGRES_USER"],
        password=values["POSTGRES_PASSWORD"],
    )
    try:
        with blocker.cursor() as cursor:
            cursor.execute(f'SELECT * FROM "{MART}".fct_gl_entry')

        started = time.monotonic()
        with pytest.raises(promote.PromotionBlocked) as raised:
            promote.promote(db, MART, build)
        waited = time.monotonic() - started

        assert MART in str(raised.value)
        # The blocker is still holding its lock, so a promotion without the timeout does
        # not fail late - it never returns at all, and the assertion above would be
        # reached by nobody. Timing it turns that hang into a report.
        assert waited < 15, f"waited {waited:.1f}s for a {promote.LOCK_TIMEOUT} timeout"
    finally:
        blocker.rollback()
        blocker.close()

    assert marker(db, MART, "fct_gl_entry") == MART
    assert build in schemas(db, MART)


@db_test
def test_the_promotion_commits_on_a_connection_that_is_not_in_autocommit(db, clean):
    """33. `db.connection_from` hands back a connection that is not in autocommit, which
    is what `build_and_promote` opens for itself. On one of those, psycopg opens an
    implicit transaction at the first statement, and `transaction()` inside one is a
    SAVEPOINT: it releases on exit and commits nothing, so closing the connection rolls
    the whole promotion back. Everything reports success and the mart is untouched.

    Not in the design's case list. It is here because the implementation had it: the
    fixture's connection is in autocommit, so cases 13-16 passed while the pipeline
    promoted nothing at all.
    """
    make_schema(db, MART, "fct_gl_entry")
    build = promote.build_schema(MART, RUN)
    db.commit()

    values = settings()
    worker = transform_db.connect(
        host=values["POSTGRES_HOST"], port=values["POSTGRES_PORT"],
        dbname=values["POSTGRES_DB"], user=values["POSTGRES_USER"],
        password=values["POSTGRES_PASSWORD"],
    )
    assert not worker.autocommit
    try:
        promote.prepare(worker, MART, build)
        with worker.cursor() as cursor:
            cursor.execute(f'CREATE TABLE "{build}".fct_gl_entry (marker text)')
            cursor.execute(f'INSERT INTO "{build}".fct_gl_entry VALUES (%s)', (build,))
        worker.commit()
        promote.promote(worker, MART, build)
        promote.sweep(worker, MART, keep=build)
    finally:
        worker.close()

    assert marker(db, MART, "fct_gl_entry") == build


@db_test
def test_a_connection_already_in_a_transaction_is_refused(db, clean):
    """34. Not in the design's case list; the codex review of the code asked for it.

    Case 33's defect has a second door: a caller that ran anything on the connection
    first. `transaction()` is then a savepoint again, with the same silent result. The
    guard turns it into a refusal, which is the only reading of it anybody can act on.
    """
    make_schema(db, MART)
    db.commit()

    values = settings()
    worker = transform_db.connect(
        host=values["POSTGRES_HOST"], port=values["POSTGRES_PORT"],
        dbname=values["POSTGRES_DB"], user=values["POSTGRES_USER"],
        password=values["POSTGRES_PASSWORD"],
    )
    try:
        with worker.cursor() as cursor:
            cursor.execute("SELECT 1")  # psycopg is now inside an implicit transaction

        with pytest.raises(promote.ConnectionNotIdle):
            promote.prepare(worker, MART, promote.build_schema(MART, RUN))
    finally:
        worker.rollback()
        worker.close()

    assert promote.build_schema(MART, RUN) not in schemas(db, MART)


def test_a_connection_to_another_database_is_refused(monkeypatch):
    """35. Not in the design's case list; the codex review of the code asked for it.

    The DDL runs on the connection handed in and the models are built by a subprocess
    that connects for itself. Two different databases means dropping a schema in one
    having built in the other, which is the one outcome docs/adr/0048 exists to prevent.
    """
    from pipeline import dbt as pipeline_dbt
    from transform import db as transform_db_module

    monkeypatch.setattr(transform_db_module, "settings",
                        lambda *a, **k: {**transform_db_module.DEFAULTS,
                                         "POSTGRES_DB": "somewhere_else"})

    class Elsewhere:
        info = SimpleNamespace(dbname="not_somewhere_else")

    with pytest.raises(pipeline_dbt.WrongDatabase) as raised:
        pipeline_dbt.build_and_promote(
            landing="l", mart="m", run_id=RUN, connection=Elsewhere())

    assert "somewhere_else" in str(raised.value)
    assert "not_somewhere_else" in str(raised.value)

