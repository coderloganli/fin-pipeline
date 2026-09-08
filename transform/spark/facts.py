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

__all__ = ["MODEL", "Unattributed", "Multiplied", "AMOUNT_PRECISION", "RATE_PRECISION",
           "BASE_PRECISION", "build", "read", "frame", "checksum", "main"]

MODEL = "fct_gl_entry"

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


def staging_path(staging_dir) -> Path:
    return Path(staging_dir) / MODEL


def build(spark, raw_dir, staging_dir) -> Path:
    """Read the entries and the three chained dimensions, attribute, write."""
    from pyspark.sql import functions as F

    gl = contracts.load("gl_entry")
    entries = (
        # The table directory, not one partition: `gl_entry` is partitioned by
        # accounting period, and Spark discovers those directories itself.
        spark.read.parquet(str(raw.table_dir(raw_dir, gl["table"])))
        .select(*raw.columns_of(gl), raw.FIRST_RUN_ID, raw.LAST_RUN_ID)
        .withColumn("accounting_date", F.to_date("accounting_date"))
        .withColumn("posted_at", F.to_date("posted_at"))
        .withColumn("amount_dr", F.col("amount_dr").cast(AMOUNT_TYPE))
        .withColumn("amount_cr", F.col("amount_cr").cast(AMOUNT_TYPE))
        .withColumnRenamed(raw.FIRST_RUN_ID, "source_first_run_id")
        .withColumnRenamed(raw.LAST_RUN_ID, "source_last_run_id")
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
            f"{missing} entries matched no account, cost centre or rate on their "
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

    target = staging_path(staging_dir)
    (
        priced.select(
            "entry_id", "version", "accounting_date", "posted_at",
            "account_key", "cost_center_key", "fx_key",
            "account_code", "account_type", "parent_code",
            "cost_center_code", "dept_code",
            "currency", "rate_to_base",
            "amount_dr", "amount_cr", "amount_dr_base", "amount_cr_base",
            "doc_id", "vendor_code", "description",
            "source_first_run_id", "source_last_run_id",
        )
        .coalesce(1).write.mode("overwrite").parquet(str(target))
    )
    return target


def frame(spark, staging_dir):
    return spark.read.parquet(str(staging_path(staging_dir)))


def read(spark, staging_dir) -> list[dict]:
    return [row.asDict() for row in frame(spark, staging_dir).collect()]


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
    args = parser.parse_args(argv)

    borrowed = session.active() is not None
    spark = session.build("fin-pipeline-facts")
    try:
        target = build(spark, args.raw, args.staging)
        print(f"{MODEL} -> {target}")
    finally:
        if not borrowed:
            spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
