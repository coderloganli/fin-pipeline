"""Attributing each entry to the structure that was in force on its accounting date.

Three joins, one shape, used three times:

    <natural key> = <natural key>  AND  accounting_date BETWEEN valid_from AND valid_to

Both halves, every time. With the range alone every entry matches every version valid
on its date, which does not raise - it multiplies, and the result still adds up to a
number somebody might publish. See docs/adr/0029.

Amounts and rates are read as decimals, never as doubles: docs/adr/0013 spent a whole
ticket keeping floats out of rate generation, and reading them back as doubles would
undo it. The base-currency figure is rounded at the line, because the monthly aggregate
has to agree with the entries added up. See docs/adr/0031.

    python -m transform.spark.facts --raw data/raw --staging data/staging
"""

import argparse
import hashlib
from pathlib import Path

from ingest import contracts, raw
from transform.spark import scd2

__all__ = ["MODEL", "ADJUSTMENT_MODEL", "MODELS", "SOURCES", "Unattributed",
           "Multiplied", "AMOUNT_PRECISION", "RATE_PRECISION", "BASE_PRECISION",
           "build", "read", "frame", "checksum", "partitions", "main"]

MODEL = "fct_gl_entry"
ADJUSTMENT_MODEL = "fct_gl_adjustment"

# Two facts, one attribution. An adjustment is not a row of `fct_gl_entry`: gate 3
# requires the rows sharing a `doc_id` to balance, and an adjustment is a single-sided
# delta against a voucher that already balanced, so folding it in would turn a gate red
# on correct data. See docs/adr/0042.
MODELS = {"gl_entry": MODEL, "gl_adjustment": ADJUSTMENT_MODEL}
SOURCES = {model: table for table, model in MODELS.items()}

# The columns an adjustment carries and an entry does not: which line it revises, and
# whether it amends the period's figure or restates it. `adjusts_entry_id` is a trail to
# follow rather than a constraint - see docs/adr/0042.
ADJUSTMENT_COLUMNS = ("adjusts_entry_id", "adjustment_type")

# The column the staging facts are partitioned by, derived from the accounting date the
# same way the raw layer derives its own. See docs/adr/0026.
PARTITION = "accounting_period"

# Measured, not assumed: decimal(18,2) * decimal(18,6) is decimal(37,8) in Spark, and
# round(_, 2) reduces that to decimal(32,2). 37 is one short of Spark's maximum of 38,
# and at the maximum Spark reduces the scale rather than raising - a quiet loss of
# precision in the one calculation docs/adr/0031 exists to protect. The tests assert
# these exactly rather than asserting they are large enough.
AMOUNT_PRECISION = 18
RATE_PRECISION = 18
BASE_PRECISION = 32

AMOUNT_TYPE = f"decimal({AMOUNT_PRECISION},2)"
RATE_TYPE = f"decimal({RATE_PRECISION},6)"

# How much of a failure to show. A build that lost a quarter of the ledger should say
# so and stop, not print a quarter of a million entry ids.
SAMPLE = 5


class Multiplied(RuntimeError):
    """A join matched an entry more than once.

    The orphan check below cannot see this: every key is non-null, every row looks
    attributed, and the table simply has more rows than the ledger has entries. It
    still totals to a number somebody might publish, which is why it is a gate here
    rather than an assertion in a test over well-formed fixtures. Overlapping intervals
    in a staged dimension are what produce it. See docs/adr/0029.
    """


class Unattributed(RuntimeError):
    """Entries that matched no account, cost centre or rate.

    A fact table quietly missing its base-currency amounts is worse than a build that
    stops: the figures that remain still total to something publishable. See
    docs/product.md, breaking is better than drifting.
    """


def staging_path(staging_dir, *, model: str = MODEL) -> Path:
    return Path(staging_dir) / model


def partitions(staging_dir, *, model: str = MODEL) -> list[str]:
    """The accounting periods this model currently holds on disk."""
    root = staging_path(staging_dir, model=model)
    if not root.is_dir():
        return []
    return sorted(
        directory.name.split("=", 1)[1]
        for directory in root.glob(f"{PARTITION}=*") if directory.is_dir()
    )


def write_partitions(frame_, staging_dir, *, model: str, periods) -> Path:
    """Write one partition per period, each by overwriting its own directory.

    Not `partitionBy` over the whole frame. Spark's
    `spark.sql.sources.partitionOverwriteMode` defaults to `STATIC`, under which
    `mode("overwrite")` on a partitioned write replaces the entire table directory - so
    a backfill of one period would silently rewrite every other one, and the property
    this whole ticket is judged on would be false with nothing raised. Writing the
    directory makes the guarantee structural: this cannot touch a partition it was not
    given. See docs/adr/0041.

    A period whose filter leaves no rows has its directory removed rather than written
    empty, which is `raw.write_partition`'s rule and holds here for the same reason: a
    zero-row file makes the layout claim a period exists that holds nothing.
    """
    import shutil

    from pyspark.sql import functions as F

    root = staging_path(staging_dir, model=model)
    root.mkdir(parents=True, exist_ok=True)
    periods = sorted(periods)
    if not periods:
        return root

    # Cached, and the non-empty periods asked for once. Without this every partition
    # write re-evaluates the whole plan - for the aggregate that is the dense grid, the
    # dimension joins, the window functions and the group-by - so a dirty set of N
    # periods would recompute the answer N times over, plus an emptiness job each. One
    # pass, then N writes off the cached result.
    frame_ = frame_.persist()
    try:
        present = {
            row[PARTITION]
            for row in frame_.select(PARTITION).distinct().collect()
        }
        for period in periods:
            directory = root / f"{PARTITION}={period}"
            if period not in present:
                # The period emptied out. A zero-row file would make the layout claim a
                # period exists that holds nothing, which is `raw.write_partition`'s
                # rule and holds here for the same reason.
                if directory.is_dir():
                    shutil.rmtree(directory)
                continue
            (
                frame_.where(F.col(PARTITION) == period).drop(PARTITION)
                .coalesce(1).write.mode("overwrite").parquet(str(directory))
            )
    finally:
        frame_.unpersist()
    return root


def build(spark, raw_dir, staging_dir, *, model: str = MODEL, dirty=None) -> Path:
    """Read one fact's source and the three chained dimensions, attribute, write.

    `dirty` is the set of accounting periods to rebuild. None is every period the raw
    layer holds, which is the behaviour this had before selective recomputation and is
    still what a full run does.
    """
    from pyspark.sql import functions as F

    gl = contracts.load(SOURCES[model])
    carried = list(ADJUSTMENT_COLUMNS) if model == ADJUSTMENT_MODEL else []
    source = read_source(spark, raw_dir, gl, dirty)
    entries = (
        source
        .select(*raw.columns_of(gl), raw.FIRST_RUN_ID, raw.LAST_RUN_ID)
        .withColumn("accounting_date", F.to_date("accounting_date"))
        .withColumn("posted_at", F.to_date("posted_at"))
        .withColumn("amount_dr", F.col("amount_dr").cast(AMOUNT_TYPE))
        .withColumn("amount_cr", F.col("amount_cr").cast(AMOUNT_TYPE))
        .withColumnRenamed(raw.FIRST_RUN_ID, "source_first_run_id")
        .withColumnRenamed(raw.LAST_RUN_ID, "source_last_run_id")
        # Derived rather than read off the directory name, so the staging partition is
        # a function of the row and cannot disagree with where raw put it.
        .withColumn(PARTITION, F.date_format("accounting_date", "yyyy-MM"))
    )

    def attributed(left, contract, left_key, carry, prefix):
        """One join: the natural key, and the date inside the version's interval."""
        _, natural_key, _ = scd2.MODELS[contract["table"]]
        right = scd2.frame(spark, contract, staging_dir).select(
            F.col("surrogate_key").alias(f"{prefix}_key"),
            F.col(natural_key[0]).alias("_natural"),
            F.col("valid_from").alias("_from"),
            F.col("valid_to").alias("_to"),
            *[F.col(name) for name in carry],
        )
        return left.join(
            F.broadcast(right),
            (F.col(left_key) == F.col("_natural"))
            & F.col("accounting_date").between(F.col("_from"), F.col("_to")),
            "left",
        ).drop("_natural", "_from", "_to")

    joined = attributed(entries, contracts.load("dim_account_src"), "account_code",
                        ["account_type", "parent_code"], "account")
    joined = attributed(joined, contracts.load("dim_cost_center_src"),
                        "cost_center_code", ["dept_code"], "cost_center")
    joined = attributed(joined, contracts.load("fx_rate"), "currency",
                        ["rate_to_base"], "fx")

    joined = joined.withColumn("rate_to_base", F.col("rate_to_base").cast(RATE_TYPE))

    orphans = joined.where(
        F.col("account_key").isNull()
        | F.col("cost_center_key").isNull()
        | F.col("fx_key").isNull()
    )
    if missing := orphans.count():
        sample = [row["entry_id"] for row in orphans.select("entry_id").limit(SAMPLE).collect()]
        raise Unattributed(
            f"{missing} rows of {model} matched no account, cost centre or rate on their "
            f"accounting date, so they would carry no base-currency amount. "
            f"For example: {', '.join(sample)}. A dimension that does not cover an "
            f"entry's date is the failure this check exists for - see docs/adr/0029."
        )

    # Both halves of every join predicate held, or this is not the ledger. `scd2`
    # builds intervals that cannot overlap, but that is a property of another module
    # and this is the layer whose output would be wrong.
    landed, unique = joined.count(), joined.select("entry_id", "version").distinct().count()
    if landed != unique:
        raise Multiplied(
            f"the attribution produced {landed} rows for {unique} entries. A join "
            f"matched some entry more than once, which means a dimension carries "
            f"overlapping validity intervals for one natural key. See docs/adr/0029."
        )

    priced = (
        joined
        .withColumn("amount_dr_base", F.round(F.col("amount_dr") * F.col("rate_to_base"), 2))
        .withColumn("amount_cr_base", F.round(F.col("amount_cr") * F.col("rate_to_base"), 2))
    )

    selected = priced.select(
        "entry_id", "version", "accounting_date", "posted_at",
        "account_key", "cost_center_key", "fx_key",
        "account_code", "account_type", "parent_code",
        "cost_center_code", "dept_code",
        "currency", "rate_to_base",
        "amount_dr", "amount_cr", "amount_dr_base", "amount_cr_base",
        "doc_id", *carried, "vendor_code", "description",
        "source_first_run_id", "source_last_run_id",
        PARTITION,
    )
    # Which periods to write: the ones asked for, or every period the source holds. A
    # dirty period the source no longer has rows for is still written - as a removal.
    writing = set(dirty) if dirty is not None else set(
        row[PARTITION] for row in selected.select(PARTITION).distinct().collect()
    )
    return write_partitions(selected, staging_dir, model=model, periods=writing)


def read_source(spark, raw_dir, contract: dict, dirty=None):
    """The raw rows to attribute: every partition, or only the dirty ones.

    An entry's attribution is a function of that entry and the dimensions and nothing
    else, so unlike the aggregate this can scope its read as narrowly as its write. See
    docs/adr/0041.
    """
    directory = raw.table_dir(raw_dir, contract["table"])
    if dirty is None:
        # The table directory, not one partition: the raw layer is partitioned by
        # accounting period, and Spark discovers those directories itself.
        if not any(directory.rglob("*.parquet")):
            return empty_source(spark, contract)
        return spark.read.parquet(str(directory))

    # A directory that holds no Parquet file is not a partition to read. It occurs
    # while a period is being emptied out, and handing it to Spark raises
    # UNABLE_TO_INFER_SCHEMA rather than reading nothing.
    paths = [
        str(directory / f"{PARTITION}={period}")
        for period in sorted(dirty)
        if any((directory / f"{PARTITION}={period}").glob("*.parquet"))
    ]
    if not paths:
        return empty_source(spark, contract)
    # basePath so Spark still reads `accounting_period` off the directory names when it
    # is handed the partitions rather than the table.
    return spark.read.option("basePath", str(directory)).parquet(*paths)


def empty_source(spark, contract: dict):
    """No partitions to read. A frame with the right columns rather than a raise: a
    dirty period the source has emptied out is a removal, not an error."""
    from pyspark.sql import types as T

    names = [*raw.columns_of(contract), raw.FIRST_RUN_ID, raw.LAST_RUN_ID, PARTITION]
    schema = T.StructType([T.StructField(name, T.StringType(), True) for name in names])
    return spark.createDataFrame([], schema)


def frame(spark, staging_dir, *, model: str = MODEL):
    return spark.read.parquet(str(staging_path(staging_dir, model=model)))


def read(spark, staging_dir, *, model: str = MODEL) -> list[dict]:
    return [row.asDict() for row in frame(spark, staging_dir, model=model).collect()]


def checksum(rows) -> str:
    """Over rows rather than bytes, for the reason docs/adr/0017 gives: Spark names its
    output files itself and Parquet carries its writer's version."""
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


def main(argv: list[str] | None = None) -> int:
    from transform.spark import session

    parser = argparse.ArgumentParser(prog="transform.spark.facts", description=__doc__)
    parser.add_argument("--raw", default="data/raw")
    parser.add_argument("--staging", default="data/staging")
    parser.add_argument("--models", nargs="*", default=None,
                        help=f"which facts to build; defaults to all of {sorted(SOURCES)}")
    args = parser.parse_args(argv)

    models = args.models or sorted(SOURCES)
    unknown = [model for model in models if model not in SOURCES]
    if unknown:
        parser.error(f"unknown models {unknown}; this module builds {sorted(SOURCES)}")

    with session.acquire("fin-pipeline-facts") as spark:
        for model in models:
            target = build(spark, args.raw, args.staging, model=model)
            print(f"{model} -> {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
