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

    # Collapse a version that restates the previous one without changing anything. A
    # new effective date carrying identical attributes is not a change, and counting it
    # as one would make "how many versions does this key have" a function of how many
    # times the source re-exported it. The attribute hash is what the ticket's
    # "compare a hash of the attributes" becomes once the source declares its own dates.
    ordered = Window.partitionBy(*natural_key).orderBy(effective)
    fingerprint = F.sha2(_rendered(attributes), 256) if attributes else F.lit("")
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

    # Stop only a session this call started. `build` goes through `getOrCreate`, so
    # when something in this process already has one, that is what comes back and
    # stopping it would take it away from its owner.
    borrowed = session.active() is not None
    spark = session.build("fin-pipeline-scd2")
    try:
        for table in tables:
            contract = contracts.load(table)
            target = build(spark, contract, args.raw, args.staging)
            print(f"{table}: {MODELS[table][0]} -> {target}")
    finally:
        if not borrowed:
            spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
