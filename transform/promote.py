"""The promotion boundary: a mart is renamed into place, never written in place.

dbt builds the models and then runs the gates over them, so a gate that goes red does so
after the table it guards has been written. `dbt build` stops there and exits non-zero,
and the failing figures sit in the mart schema until the next successful build replaces
them. The six gates are not the problem - what was missing is that catching a failure
and publishing it were not mutually exclusive.

So a run builds into `<mart>__b<run_id>` and that schema becomes the mart by being
renamed. Nothing copies rows at promotion time: `ALTER SCHEMA ... RENAME TO` moves a
namespace, and Postgres makes DDL transactional, so dropping the old mart and renaming
the new one is one commit. A reader sees the mart it had or the mart it is getting.

Four functions, in the order a run calls them:

    build = build_schema(mart, run_id)
    prepare(connection, mart, build)      # a clean schema, carrying the drift snapshot
    ...                                   # dbt builds into it and the gates run
    promote(connection, mart, build)      # green: the rename
    sweep(connection, mart, keep=build)   # green: the discards, now resolved

A build that fails calls neither of the last two. Its schema stays, named in the run
record, with its offending rows still readable in `<build>_dbt_test__audit`.

See docs/adr/0048.
"""

from __future__ import annotations

__all__ = [
    "AUDIT_SUFFIX",
    "BUILD_INFIX",
    "MAX_IDENTIFIER",
    "RUN_ID_LENGTH",
    "MART_SCHEMA_BUDGET",
    "LOCK_TIMEOUT",
    "SchemaNameTooLong",
    "NothingToPromote",
    "PromotionBlocked",
    "ConnectionNotIdle",
    "build_schema",
    "build_schemas",
    "prepare",
    "promote",
    "sweep",
]

# dbt appends this to the target schema when `store_failures` is on, which it is
# project-wide. It is part of the budget rather than an afterthought: a name that fits on
# its own and not with the suffix is worse than one that is simply too long, because dbt
# computes the full name, Postgres stores the truncated one, the relation cache misses,
# and the second build in that schema fails with "relation already exists" on a test that
# is perfectly correct.
AUDIT_SUFFIX = "_dbt_test__audit"

# What marks a schema as one build's. Two underscores rather than one so the mart's own
# audit schema, `<mart>_dbt_test__audit`, cannot be read as a build schema.
BUILD_INFIX = "__b"

# Postgres truncates an identifier at 63 *bytes*, silently. Bytes rather than characters,
# which is why the budget is measured in UTF-8 below: a name of 21 non-ASCII characters
# passes a length check and is truncated anyway.
MAX_IDENTIFIER = 63

# `YYYYMMDDTHHMMSSZ-xxxxxx`, fixed by docs/adr/0019 and `ingest.runs.new_run_id`.
RUN_ID_LENGTH = 23

MART_SCHEMA_BUDGET = MAX_IDENTIFIER - len(AUDIT_SUFFIX) - len(BUILD_INFIX) - RUN_ID_LENGTH

# How long a statement here will wait for the locks it needs. `tests/conftest.py` records
# what happens without a bound: a connection sitting idle in a transaction holds a lock
# on what it read, which blocks DDL indefinitely and with nothing raised. A promotion
# that hangs is worse than one that fails, because the run never reports at all. The
# sweep is bounded for the same reason and a sharper one: an operator reading a retained
# failed schema is exactly the lock the sweep will meet.
LOCK_TIMEOUT = "30s"


class SchemaNameTooLong(ValueError):
    """A mart schema that leaves no room for the build and audit suffixes."""


class NothingToPromote(LookupError):
    """The build schema is not there.

    Raised before anything is dropped. The caller has lost track of its own run, and
    dropping the mart for it would destroy the figures the promotion exists to protect.
    """


class PromotionBlocked(RuntimeError):
    """Something else holds a lock on the mart and would not let go in time."""


class ConnectionNotIdle(RuntimeError):
    """The connection handed in is already inside a transaction.

    Refused rather than worked around, because the failure it produces is silent.
    psycopg's `transaction()` inside an open transaction is a SAVEPOINT: it releases on
    exit and commits nothing. Every function here would report success, dbt would build
    into a schema no other connection can see, and closing the connection would roll the
    whole promotion back.
    """


# --- names ------------------------------------------------------------------

def build_schema(mart: str, run_id: str) -> str:
    """The schema this run builds into.

    The run id goes in verbatim, so the schema name is what somebody pastes into
    `python -m ingest.runs --run` while working out what a failed build caught.
    """
    name = f"{mart}{BUILD_INFIX}{run_id}"
    length = len((name + AUDIT_SUFFIX).encode("utf-8"))
    if length > MAX_IDENTIFIER:
        raise SchemaNameTooLong(
            f"mart schema {mart!r} is {len(mart.encode('utf-8'))} bytes and the budget "
            f"is {MART_SCHEMA_BUDGET}: with {BUILD_INFIX!r}, a {RUN_ID_LENGTH}-character "
            f"run id and dbt's {AUDIT_SUFFIX!r}, {name!r} would reach {length} bytes, "
            f"past Postgres's {MAX_IDENTIFIER} - and be truncated without a word."
        )
    return name


def _like(name: str) -> str:
    """A LIKE pattern matching this name and anything suffixed to it.

    `_` is a single-character wildcard, and every name here is full of them. Unescaped,
    `mart__b%` also matches `martxybogus`.
    """
    escaped = name.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%")
    return escaped + "%"


# --- statements ---------------------------------------------------------------
#
# Every function below does all of its work inside one `connection.transaction()`, and
# executes nothing before entering it. That is not style. On a connection that is not in
# autocommit - which is what `db.connection_from` hands back - psycopg opens an implicit
# transaction at the first statement, and `transaction()` inside one is a SAVEPOINT
# rather than a transaction: it releases on exit and commits nothing, so closing the
# connection rolls the whole promotion back. A stray read before the block is enough to
# turn a promotion into a no-op that reports success. `_working` is the guard that says
# so out loud rather than leaving it to whoever edits next.
#
# Identifiers are composed with `psycopg.sql.Identifier` rather than interpolated into a
# quoted f-string. These are `DROP SCHEMA ... CASCADE` statements and the names reach
# them from configuration; a name carrying a double quote would not merely fail.

from contextlib import contextmanager


@contextmanager
def _working(connection):
    """One transaction, from an idle connection, with the lock wait bounded."""
    from psycopg import pq, sql

    if connection.info.transaction_status != pq.TransactionStatus.IDLE:
        raise ConnectionNotIdle(
            "this connection is already inside a transaction. transform.promote needs "
            "to commit its own, and psycopg's transaction() inside an open one is a "
            "savepoint that commits nothing - so every statement here would report "
            "success and be rolled back when the connection closed. Commit or roll "
            "back first, or hand in a connection of its own."
        )
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(LOCK_TIMEOUT))
            )
            yield cursor


def _drop(cursor, *schemas: str) -> None:
    from psycopg import sql

    for schema in schemas:
        cursor.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
        )


def _exists(cursor, schema: str) -> bool:
    cursor.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema,))
    return cursor.fetchone() is not None


def _build_schemas(cursor, mart: str) -> list[str]:
    """This mart's build schemas, sorted, without their audit schemas.

    A build schema's own audit schema shares its prefix. Returning it here would have
    `sweep` drop it twice and `promote` reach for a schema that had already gone.
    """
    cursor.execute(
        r"SELECT nspname FROM pg_namespace WHERE nspname LIKE %s ESCAPE '\'",
        (_like(f"{mart}{BUILD_INFIX}"),),
    )
    found = [row[0] for row in cursor.fetchall()]
    return sorted(name for name in found if not name.endswith(AUDIT_SUFFIX))


def build_schemas(connection, mart: str) -> list[str]:
    """This mart's build schemas, sorted, without their audit schemas."""
    with _working(connection) as cursor:
        return _build_schemas(cursor, mart)


# --- the three steps ---------------------------------------------------------

def prepare(connection, mart: str, build: str) -> None:
    """A clean build schema, carrying the drift gate's memory into it.

    Dropped first, because an Airflow task that retries carries the same run id and
    therefore the same build schema: appending to what the failed attempt left would
    double every count.

    `model_row_count` is copied because it is the one thing in the mart that cannot be
    recomputed from its inputs - it is a record of builds rather than a derivation of
    them (docs/adr/0036). Copying it in rather than keeping it outside the swap is what
    makes the baseline a history of builds that were *accepted*: a build that appends
    its row and then fails a gate takes that row away with the schema it was discarded
    in. dbt sees a table already in the schema and appends to it, because
    `is_incremental()` reads `adapter.get_relation` and the materialization takes
    `existing_relation` from the cache dbt fills before the first model runs.
    """
    from psycopg import sql

    with _working(connection) as cursor:
        _drop(cursor, build, build + AUDIT_SUFFIX)
        cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(build)))
        cursor.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s",
            (mart, "model_row_count"),
        )
        if cursor.fetchone() is not None:
            cursor.execute(
                sql.SQL("CREATE TABLE {}.model_row_count AS "
                        "SELECT * FROM {}.model_row_count").format(
                    sql.Identifier(build), sql.Identifier(mart))
            )


def promote(connection, mart: str, build: str) -> None:
    """Rename the build schema into place, and its audit schema with it.

    One transaction, so a reader sees the mart it had or the mart it is getting. The
    audit schema moves too, and the old one goes whether or not there is a new one to
    replace it: a mart whose `_dbt_test__audit` belonged to some earlier build would
    answer "what failed" with a failure that has nothing to do with the figures beside
    it.
    """
    import psycopg
    from psycopg import sql

    try:
        with _working(connection) as cursor:
            if not _exists(cursor, build):
                raise NothingToPromote(
                    f"there is no schema {build!r} to promote; {mart!r} is left alone. "
                    f"A build schema that is not there means the caller has lost track "
                    f"of its run, and dropping the mart for it would destroy the "
                    f"figures this promotion exists to protect."
                )
            had_audit = _exists(cursor, build + AUDIT_SUFFIX)
            _drop(cursor, mart, mart + AUDIT_SUFFIX)
            cursor.execute(sql.SQL("ALTER SCHEMA {} RENAME TO {}").format(
                sql.Identifier(build), sql.Identifier(mart)))
            if had_audit:
                cursor.execute(sql.SQL("ALTER SCHEMA {} RENAME TO {}").format(
                    sql.Identifier(build + AUDIT_SUFFIX),
                    sql.Identifier(mart + AUDIT_SUFFIX)))
    except psycopg.errors.LockNotAvailable as failure:
        raise PromotionBlocked(
            f"could not take the locks to promote {build!r} into {mart!r} within "
            f"{LOCK_TIMEOUT}. Something else is holding {mart!r} - most often a "
            f"connection left idle in a transaction after reading from it. The mart is "
            f"unchanged and the build schema is still there. Driver said: {failure}"
        ) from failure


def sweep(connection, mart: str, *, keep: str) -> list[str]:
    """Drop this mart's build schemas other than `keep`. Returns what went.

    Called after a promotion rather than before a build, and that is the whole of why it
    needs no ordering. A sweep that kept "the newest discard" would have to know which
    one that is, and it cannot: docs/adr/0019 gives a run id a timestamp to the second
    and a random suffix, and `ingest/runs.py` says two ids from the same second sort
    arbitrarily with respect to each other.

    Sweeping on success is also the better rule for the reader it exists for. Under the
    other one a failure's evidence is destroyed by the very next attempt at the thing
    that failed, which is usually minutes later and before anyone has looked.
    """
    import psycopg

    try:
        with _working(connection) as cursor:
            dropped = [name for name in _build_schemas(cursor, mart) if name != keep]
            for schema in dropped:
                _drop(cursor, schema, schema + AUDIT_SUFFIX)
    except psycopg.errors.LockNotAvailable as failure:
        raise PromotionBlocked(
            f"could not take the locks to sweep {mart!r}'s discarded build schemas "
            f"within {LOCK_TIMEOUT}. Somebody is most likely reading one of them, which "
            f"is what they are kept for. Nothing was swept; the next successful build "
            f"will try again. Driver said: {failure}"
        ) from failure
    return dropped
