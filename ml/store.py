"""The table the flags land in, and how they get there.

`anomaly_flag` lives in a schema of its own, outside the mart. Two reasons, and the
second is the one that decides it: a promotion drops the mart schema and renames a build
into place (docs/adr/0048), so a table of ours inside it would not survive the next
build; and this layer must not be able to stop the mart being published, which putting
it inside the promotion transaction would let it do. See docs/adr/0050.

psycopg and explicit SQL, the way `transform/load.py` writes the landing schema. Nothing
here is a derivation dbt could express: these rows are model output.
"""

from psycopg import sql

__all__ = ["TABLE", "COLUMNS", "MartUnavailable", "ensure_schema", "replace_periods"]

TABLE = "anomaly_flag"

# `model_family` is in the key because both arms can write a flag for the same balance.
# Without it the second write would either violate the key or silently replace the first
# arm's result, and a comparison whose two halves overwrite each other is not one.
KEY = ("account_code", "cost_center_code", "accounting_period", "model_family")

COLUMNS = (
    "account_code", "cost_center_code", "accounting_period",
    "actual", "predicted", "lower_bound", "upper_bound",
    "residual", "score", "side", "model_family", "nominal_coverage", "run_id",
)

DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    account_code       text    NOT NULL,
    cost_center_code   text    NOT NULL,
    accounting_period  text    NOT NULL,
    actual             numeric NOT NULL,
    predicted          numeric NOT NULL,
    lower_bound        numeric NOT NULL,
    upper_bound        numeric NOT NULL,
    residual           numeric NOT NULL,
    score              numeric NOT NULL,
    side               text    NOT NULL,
    model_family       text    NOT NULL,
    nominal_coverage   numeric NOT NULL,
    run_id             text    NOT NULL,
    PRIMARY KEY (account_code, cost_center_code, accounting_period, model_family)
)
"""


class MartUnavailable(RuntimeError):
    """The mart this layer trains on has not been built.

    Named rather than left to psycopg, for the reason `transform/db.py` names the
    command that starts Postgres: the caller can do something about it, and the
    driver's message does not say what.
    """


def ensure_schema(connection, schema: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
        )
        cursor.execute(
            sql.SQL(DDL).format(table=sql.Identifier(schema, TABLE))
        )
    connection.commit()


def read_balances(connection, mart_schema: str, *, model: str = "agg_monthly_balance"):
    """The whole monthly grid. The model needs the history, not the judged period.

    Every period is read even when one is being judged, because a lag of twelve is a
    lag of twelve: a query narrowed to the period in front of it would leave the design
    matrix with nothing to build a row from.
    """
    from ml.features import Balance

    statement = sql.SQL(
        "SELECT account_code, cost_center_code, accounting_period, balance_as_restated "
        "FROM {} ORDER BY account_code, cost_center_code, accounting_period"
    ).format(sql.Identifier(mart_schema, model))

    import psycopg

    try:
        with connection.cursor() as cursor:
            cursor.execute(statement)
            rows = cursor.fetchall()
    # Only the absence of the table. A blanket `except Exception` would report a
    # permissions problem, a dropped connection or a bug in this query as "the mart has
    # not been built", and send somebody to rebuild a mart that was already there.
    except (psycopg.errors.UndefinedTable, psycopg.errors.InvalidSchemaName) as failure:
        connection.rollback()
        raise MartUnavailable(
            f"no {model} in schema {mart_schema!r}. The anomaly model trains on the "
            f"mart rather than the landing layer (docs/adr/0051), so the mart has to "
            f"have been built: `python -m pipeline daily --periods <from>:<to>`. "
            f"Postgres said: {failure}"
        ) from failure

    return [
        Balance(account_code=a, cost_center_code=c, accounting_period=p, balance=b)
        for a, c, p, b in rows
    ]


def replace_periods(connection, schema: str, periods, flags, *, family: str) -> int:
    """Delete this arm's flags for these periods, then write the new ones. One
    transaction.

    Replace rather than append: a row flagged last night and inside its interval
    tonight has to leave the queue. Left behind, it is a queue entry for a figure that is
    no longer anomalous - which costs an analyst more than a missing one, because
    somebody goes and investigates it.

    Scoped to this arm, not to the period. `model_family` is in the key precisely so
    both arms can hold a flag for one balance, and a delete that ignored it would have
    judging with one arm silently discard the other's - which is the comparison this
    layer exists to make. See docs/adr/0050.
    """
    periods = list(periods)
    if not periods:
        return 0

    ensure_schema(connection, schema)
    table = sql.Identifier(schema, TABLE)

    # One transaction around the delete and every insert, entered explicitly rather than
    # left to the connection's own mode. On an autocommit connection each statement
    # would commit on its own, and an insert that failed halfway would leave the period
    # holding part of one judgement and none of the other - a queue that is neither what
    # it was nor what it should be. `transaction()` is a no-op savepoint inside an
    # existing transaction and a real one otherwise, which is the behaviour wanted here.
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("DELETE FROM {} WHERE accounting_period = ANY(%s) "
                        "AND model_family = %s").format(table),
                (periods, family),
            )
            if flags:
                columns = sql.SQL(", ").join(sql.Identifier(name) for name in COLUMNS)
                placeholders = sql.SQL(", ").join(sql.Placeholder() * len(COLUMNS))
                cursor.executemany(
                    sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                        table, columns, placeholders),
                    [tuple(getattr(flag, name) for name in COLUMNS) for flag in flags],
                )
    return len(flags)
