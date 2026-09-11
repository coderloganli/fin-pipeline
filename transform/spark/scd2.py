"""Turning the effective-dated source dimensions into validity intervals.

The source declares when each change took effect: `dim_account_src` and
`dim_cost_center_src` key on `(code, effective_date)` and carry every version in every
extract. So the history is read, not inferred - `valid_from` is the date the change
took effect in the business, never the date this pipeline noticed it. Inferring it from
run-to-run differences would put every version's `valid_from` at "today" the first time
a fresh clone ran, and every historical report would collapse to one interval.

What this produces, per natural key: intervals that abut exactly, so they neither
overlap nor leave a gap, with exactly one of them current. The version in force ends at
the sentinel 9999-12-31 rather than at null - a null end makes `BETWEEN` evaluate to
null, and the row drops out of an inner join with nothing raised. See docs/adr/0024.

The raw layer keeps every version it has ever landed, so a version the current extract
has stopped carrying still takes part in the chain. See docs/adr/0023.

    python -m transform.spark.scd2 --raw data/raw --staging data/staging

See docs/adr/0025 (the surrogate key is derived, not sequential) and 0026 (staging is
typed, and it is rebuilt rather than accumulated).
"""

import argparse
import hashlib
import sys
from pathlib import Path

from ingest import contracts, raw

__all__ = ["MODELS", "FAR_FUTURE", "build", "read", "frame", "checksum", "main"]

# The two source dimensions, and what each becomes. The effective-date column is the
# one the contract already keys on; everything else that is not the natural key or the
# effective date is an attribute of the version.
MODELS = {
    "dim_account_src": ("dim_account", ["account_code"], "effective_date"),
    "dim_cost_center_src": ("dim_cost_center", ["cc_code"], "effective_date"),
    # A rate is a slowly changing attribute of a currency, so it is loaded by this same
    # code: a rate runs from the day it was published until the day before the next
    # one, which is what puts the weekend inside Friday's interval without a rule
    # written for weekends. See docs/adr/0029.
    "fx_rate": ("dim_fx_rate", ["currency"], "rate_date"),
}

# What a contract's declared type becomes in staging. Raw holds text because it has to
# answer whether the source really said that (docs/adr/0015); staging is where the
# retyping happens (docs/adr/0026). Only the rate is not a string today, and leaving it
# text would have made 0026 true of the interval columns and of nothing else.
COLUMN_TYPES = {
    "string": "string",
    "date": "date",
    "integer": "bigint",
    "decimal": "decimal(18,6)",
}

# The open end of an interval. See docs/adr/0024.
FAR_FUTURE = "9999-12-31"


def model_of(contract: dict):
    table = contract["table"]
    if table not in MODELS:
        raise KeyError(
            f"{table} is not an effective-dated dimension. "
            f"This module builds {sorted(MODELS)}."
        )
    return MODELS[table]


def staging_path(staging_dir, contract: dict) -> Path:
    name, _, _ = model_of(contract)
    return Path(staging_dir) / name


def _rendered(columns):
    """The injective rendering docs/adr/0025 settles on, as Spark columns.

    Every field carries its length, because the separator alone is not enough: a source
    value may hold any character, that one included, and without the length prefix
    ("A", "B<US>C") and ("A<US>B", "C") render identically. This is the same rendering
    `ingest.raw.checksum` uses, and it was argued through there first.
    """
    from pyspark.sql import functions as F

    parts = []
    for column in columns:
        column = F.col(column) if isinstance(column, str) else column
        parts.append(F.concat(F.length(column).cast("string"), F.lit(":"), column))
    return F.concat_ws(raw.UNIT_SEPARATOR, *parts)


def build(spark, contract: dict, raw_dir, staging_dir) -> Path:
    """Read one source dimension out of raw and write its SCD2 model into staging."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    _, natural_key, effective = model_of(contract)
    declared = raw.columns_of(contract)
    attributes = [c for c in declared if c not in natural_key and c != effective]

    source = Path(raw.partition_path(raw_dir, contract))
    rows = spark.read.parquet(str(source)).select(*declared)

    # Cast every column to the type its contract declares. A no-op for the two source
    # dimensions, whose attributes are all strings; it is the rate that made the gap
    # visible. The effective column is handled below, where it becomes valid_from.
    for spec in contract["columns"]:
        if spec["name"] == effective:
            continue
        rows = rows.withColumn(
            spec["name"], F.col(spec["name"]).cast(COLUMN_TYPES[spec["type"]])
        )

    # Collapse a version that restates the previous one without changing anything. A
    # new effective date carrying identical attributes is not a change, and counting it
    # as one would make "how many versions does this key have" a function of how many
    # times the source re-exported it. The attribute hash is what the ticket's
    # "compare a hash of the attributes" becomes once the source declares its own dates.
    ordered = Window.partitionBy(*natural_key).orderBy(effective)
    # Rendered from the string form of each attribute: `_rendered` prefixes a length,
    # and a length is only meaningful over text. Casting first would make the
    # fingerprint depend on how Spark formats a decimal.
    fingerprint = (
        F.sha2(_rendered([F.col(name).cast("string") for name in attributes]), 256)
        if attributes else F.lit("")
    )
    distinct = (
        rows
        .withColumn("_fingerprint", fingerprint)
        .withColumn("_previous", F.lag("_fingerprint").over(ordered))
        .where(F.col("_previous").isNull() | (F.col("_previous") != F.col("_fingerprint")))
        .drop("_fingerprint", "_previous")
    )

    # The interval is built from the next version's effective date, so abutting exactly
    # - and therefore neither overlapping nor leaving a gap - is a property of the
    # construction rather than something asserted afterwards.
    chained = (
        distinct
        .withColumn("valid_from", F.to_date(F.col(effective)))
        .withColumn("_next", F.lead(F.to_date(F.col(effective))).over(ordered))
        .withColumn(
            "valid_to",
            F.when(F.col("_next").isNull(), F.to_date(F.lit(FAR_FUTURE)))
             .otherwise(F.date_sub(F.col("_next"), 1)),
        )
        .withColumn("is_current", F.col("_next").isNull())
        .drop("_next", effective)
    )

    # `valid_from` is rendered with an explicit format rather than by implicit cast, so
    # the key does not move if a session's default date formatting does.
    keyed = chained.withColumn(
        "surrogate_key",
        F.sha2(_rendered(
            list(natural_key) + [F.date_format("valid_from", "yyyy-MM-dd")]
        ), 256),
    )

    target = staging_path(staging_dir, contract)
    (
        keyed.select("surrogate_key", *natural_key, *attributes,
                     "valid_from", "valid_to", "is_current")
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(str(target))
    )
    return target


def frame(spark, contract: dict, staging_dir):
    """The model as a DataFrame, for the tests that ask about its types."""
    return spark.read.parquet(str(staging_path(staging_dir, contract)))


def read(spark, contract: dict, staging_dir) -> list[dict]:
    """The model as ordinary dicts. These tables are tens of rows."""
    return [row.asDict() for row in frame(spark, contract, staging_dir).collect()]


def checksum(rows, contract: dict) -> str:
    """A digest over the model's rows, for the same reason `raw.checksum` exists and
    rendered the same way: Spark names its output files itself and Parquet carries its
    writer's version, so bytes were never going to be the instrument. See
    docs/adr/0017 and 0026."""
    rendered = sorted(
        raw.UNIT_SEPARATOR.join(
            f"{len(str(value))}:{value}" for _, value in sorted(row.items())
        )
        for row in rows
    )
    digest = hashlib.sha256()
    for line in rendered:
        digest.update(line.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="transform.spark.scd2", description=__doc__)
    parser.add_argument("--raw", default="data/raw", help="the raw layer to read")
    parser.add_argument("--staging", default="data/staging", help="where to write")
    parser.add_argument("--table", action="append", dest="tables",
                        help="a source dimension; repeatable, default all of them")
    return parser


def main(argv: list[str] | None = None) -> int:
    from transform.spark import session

    args = build_parser().parse_args(argv)
    tables = args.tables or sorted(MODELS)
    unknown = [table for table in tables if table not in MODELS]
    if unknown:
        print(f"not an effective-dated dimension: {unknown}", file=sys.stderr)
        return 2

    # Ownership is held by the block, not worked out from a thread-local. See
    # docs/adr/0049.
    with session.acquire("fin-pipeline-scd2") as spark:
        for table in tables:
            contract = contracts.load(table)
            target = build(spark, contract, args.raw, args.staging)
            print(f"{table}: {MODELS[table][0]} -> {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
