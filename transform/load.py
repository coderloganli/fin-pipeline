"""Landing the staging layer in Postgres, so dbt has something to declare as a source.

`transform/spark/` writes Parquet and `docs/adr/0026` settles that this is what staging
is. dbt-postgres can only model tables that are already in Postgres, so something has to
move them across, and this is it: pyarrow reads, psycopg copies, and Spark never opens a
database connection.

The schema is called `landing`, not `staging`. `docs/adr/0026` already owns the name
staging for the Parquet layer, and one name for two things is the failure this
repository argues against everywhere else. `landing` holds no answer that
`data/staging/` does not already hold.

Drop, create and copy run inside one transaction. Postgres makes DDL transactional, so
a load that dies halfway leaves the previous table exactly as it was rather than a
half-filled one - which matters because the alternative is a mart quietly built on a
fraction of the ledger.

    python -m transform.load --staging data/staging --raw data/raw

See docs/adr/0034.
"""

import argparse
from pathlib import Path

from transform import db

__all__ = ["MODELS", "UnmappedType", "EmptyStagingModel", "postgres_schema",
           "copy_rows", "load_model",
           "load_all", "main"]


class UnmappedType(TypeError):
    """An Arrow type the mapping does not cover.

    Stopping is the point. Guessing a Postgres type here lands a column whose values
    are silently wrong, which is the failure shape this project keeps designing
    against - see docs/product.md, breaking is better than drifting.
    """


# Where each model is read from, and which columns are landed.
#
# `dim_vendor` is the exception in both directions. It is not effective-dated, so
# `transform/spark/scd2.py` does not build it and there is no staging form to read; and
# raw rows carry `_first_run_id` and `_last_run_id`, which are not landed, because
# ingestion metadata is reached through the run record. See docs/adr/0020 and 0034.
STAGING_MODELS = (
    "fct_gl_entry",
    "fct_gl_adjustment",
    "agg_monthly_balance",
    "dim_account",
    "dim_cost_center",
    "dim_fx_rate",
)
RAW_MODELS = {"dim_vendor": "dim_vendor"}
MODELS = STAGING_MODELS + tuple(RAW_MODELS)

# The staging models partitioned by accounting period. The column lives in the
# directory name rather than in the Parquet files, so pyarrow has to be told what type
# to give it back: left to infer, it produces a dictionary type, which `postgres_schema`
# refuses rather than guessing at - correctly, but the load would stop on a correct
# layer. See docs/adr/0026 and 0041.
PARTITIONED_MODELS = ("fct_gl_entry", "fct_gl_adjustment", "agg_monthly_balance")
PARTITION_COLUMN = "accounting_period"

# How many rows to hand the copy at a time. Large enough that the round trips do not
# dominate, small enough that a fact table does not have to be resident to be written.
BATCH = 10_000


def arrow_type(field) -> str:
    """One Arrow type to one Postgres type, or a refusal.

    Exactly, not by family. `is_date` also matches `date64` and `is_decimal` also
    matches `decimal256`, and either would land here as something Postgres accepts and
    the binary copy then writes wrongly. A mapping that answers for types it has never
    seen is not a fail-closed boundary, whatever it says in the comment above it.

    Both integer widths are listed on purpose: `agg_monthly_balance.rolling_periods` is
    a 32-bit count out of Spark and `bigint` is the right home for it.
    """
    import pyarrow as pa

    kind = field.type
    if pa.types.is_string(kind) or pa.types.is_large_string(kind):
        return "text"
    if pa.types.is_date32(kind):
        return "date"
    if pa.types.is_decimal128(kind):
        return f"numeric({kind.precision},{kind.scale})"
    if pa.types.is_int64(kind) or pa.types.is_int32(kind):
        return "bigint"
    if pa.types.is_boolean(kind):
        return "boolean"
    raise UnmappedType(
        f"column {field.name!r} is {kind}, which has no Postgres type declared here. "
        f"Add one to transform.load.arrow_type rather than letting the load guess: a "
        f"guess lands a column whose values are silently wrong."
    )


def postgres_schema(schema) -> list[tuple[str, str]]:
    """The Arrow schema as `(column, postgres type)` pairs, in order."""
    return [(field.name, arrow_type(field)) for field in schema]


def copy_types(schema) -> list[str]:
    """The type name each column is written as on the binary copy wire.

    Declared rather than inferred. psycopg reads the wire format off the first row's
    Python types, and a Python `int` is not enough to tell `int4` from `int8` - which
    is a protocol error at the first row of `agg_monthly_balance`, whose
    `rolling_periods` Spark emits as a 32-bit integer. Saying it is shorter than
    debugging it.
    """
    names = []
    for _, kind in postgres_schema(schema):
        names.append("numeric" if kind.startswith("numeric") else kind)
    return names


def parquet_dir(staging_dir, raw_dir, model: str) -> Path:
    if model in RAW_MODELS:
        from ingest import raw as raw_layer

        return raw_layer.table_dir(raw_dir, RAW_MODELS[model])
    return Path(staging_dir) / model


def columns_for(model: str) -> list[str] | None:
    """Which columns to land. None means all of them.

    A staging model is landed whole: `transform/spark/` has already decided what
    belongs in it. A raw-sourced model is projected to what its contract declares.
    """
    if model not in RAW_MODELS:
        return None
    from ingest import contracts, raw as raw_layer

    return raw_layer.columns_of(contracts.load(RAW_MODELS[model]))


class EmptyStagingModel(FileNotFoundError):
    """A partitioned staging model holds no partitions at all.

    Not an empty table: with no Parquet file anywhere under it there is no schema to
    read, so there is nothing to create the landing table from. It means the staging
    build has not run, or ran over a raw layer with no rows in it. Saying so is better
    than a message about Parquet schema inference, and stopping is better than
    inventing a shape - see docs/product.md, breaking is better than drifting.
    """


def read_table(directory: Path, columns: list[str] | None, *, partitioned: bool = False):
    """One model's Parquet, as one Arrow table.

    A partitioned model is read as a dataset with the partition field declared as a
    plain string. Hive discovery would otherwise hand back a dictionary-encoded column,
    and `postgres_schema` matches types exactly and stops on anything it has not seen -
    so a correct staging layer would fail to load, with a message about a type rather
    than about partitioning.

    Whether to read it that way comes from the model, not from what happens to be on
    disk: a partitioned model that currently holds no partitions is a different thing
    from an unpartitioned one, and guessing from the directory would send it down the
    unpartitioned path to fail on schema inference.

    The DDL, `copy_types` and `copy_rows` all read this one Arrow schema, so whatever
    order the partition column arrives in is the order all three use.
    """
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    if partitioned:
        if not any(directory.rglob("*.parquet")):
            raise EmptyStagingModel(
                f"{directory} holds no partitions, so there is no schema to land. "
                f"Run the staging build first."
            )
        partitioning = ds.HivePartitioning(
            pa.schema([(PARTITION_COLUMN, pa.string())])
        )
        table = ds.dataset(directory, format="parquet",
                           partitioning=partitioning).to_table()
    else:
        table = pq.read_table(directory)
    return table.select(columns) if columns is not None else table


def copy_rows(cursor, schema: str, model: str, table) -> None:
    """Stream the Arrow table into Postgres.

    Binary copy, so a `Decimal` arrives as a numeric rather than being re-parsed from
    text. `docs/adr/0013` spent a whole ticket keeping floats out of rate generation and
    a text round trip here would be the place it came undone.
    """
    columns = ", ".join(f'"{name}"' for name in table.column_names)
    statement = f'COPY "{schema}"."{model}" ({columns}) FROM STDIN (FORMAT BINARY)'
    with cursor.copy(statement) as copy:
        copy.set_types(copy_types(table.schema))
        for batch in table.to_batches(max_chunksize=BATCH):
            for row in zip(*[column.to_pylist() for column in batch.columns]):
                copy.write_row(row)


def load_model(connection, staging_dir, raw_dir, schema: str, model: str) -> int:
    """Drop, create and copy one model, in one transaction."""
    directory = parquet_dir(staging_dir, raw_dir, model)
    table = read_table(directory, columns_for(model),
                       partitioned=model in PARTITIONED_MODELS)
    definition = ", ".join(
        f'"{name}" {kind}' for name, kind in postgres_schema(table.schema)
    )
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            cursor.execute(f'DROP TABLE IF EXISTS "{schema}"."{model}"')
            cursor.execute(f'CREATE TABLE "{schema}"."{model}" ({definition})')
            copy_rows(cursor, schema, model, table)
    return table.num_rows


def load_all(staging_dir, raw_dir, schema: str | None = None,
             connection=None, models=None) -> dict[str, int]:
    """Every model, into the landing schema. Returns what each one landed."""
    values = db.settings()
    schema = schema or values["POSTGRES_LANDING_SCHEMA"]
    owned = connection is None
    connection = connection or db.connection_from(values)
    try:
        return {
            model: load_model(connection, staging_dir, raw_dir, schema, model)
            for model in (models or MODELS)
        }
    finally:
        if owned:
            connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="transform.load", description=__doc__)
    parser.add_argument("--staging", default="data/staging")
    parser.add_argument("--raw", default="data/raw")
    parser.add_argument("--schema", default=None,
                        help="the landing schema; defaults to POSTGRES_LANDING_SCHEMA")
    args = parser.parse_args(argv)

    landed = load_all(args.staging, args.raw, args.schema)
    for model, rows in landed.items():
        print(f"{model}: {rows} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
