"""Monthly balances: signed by the account's normal side, on a dense grid.

Every account's ordinary activity reads as a positive number that grows, so neither the
report nor the anomaly model has to know which way an account points - and the sign
comes from the account type that was in force in the period, so a reclassification does
not flip the sign of a period that already closed. See docs/adr/0032.

Every active combination carries a row for every period in range, because a
period-over-period comparison over a sparse table silently becomes a comparison with
the last month that had activity. The range is an argument rather than something
inferred from the entries: "which periods does this report cover" is a question the
data cannot answer, and deriving it from min/max would drop a leading or trailing
period in which nothing was posted anywhere. See docs/adr/0033.

    python -m transform.spark.balances --staging data/staging --periods 2026-01:2026-12
"""

import argparse
import hashlib
from pathlib import Path

from ingest import raw
from transform.spark import facts

__all__ = ["MODEL", "DEBIT_NORMAL", "build", "read", "frame", "checksum", "main"]

MODEL = "agg_monthly_balance"

# Which side an account grows on. Assets and expenses are debit-normal; liabilities,
# equity and revenue are credit-normal. See docs/adr/0032.
DEBIT_NORMAL = ("asset", "expense")

# A line's amount, and a total of them. `sum` over decimal(32,2) widens to
# decimal(38,2) in Spark, so narrowing the result back would overflow to null for a
# large enough legitimate total - silently, which is the shape of failure this project
# keeps designing against. The total keeps the width the sum produces.
MONEY = f"decimal({facts.BASE_PRECISION},2)"
TOTAL = "decimal(38,2)"
RATIO = "decimal(18,6)"

# The rolling mean is an average of cents and lands between them. It is rounded to
# cents, half-up, like every other money column here - stated because an unrounded
# mean would be the one column in this table carrying a precision the ledger does not.
ROLLING_PLACES = 2


def staging_path(staging_dir) -> Path:
    return Path(staging_dir) / MODEL


def parse_periods(text: str) -> tuple[str, str]:
    """`2026-01:2026-12` into its ends."""
    first, _, last = text.partition(":")
    if not last or first > last:
        raise ValueError(f"--periods must be FROM:TO with FROM <= TO, got {text!r}")
    return first, last


def build(spark, staging_dir, periods: str | None = None) -> Path:
    """Aggregate the fact table into monthly balances over a dense grid."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    entries = facts.frame(spark, staging_dir).withColumn(
        "accounting_period", F.date_format("accounting_date", "yyyy-MM")
    )

    if periods:
        first, last = parse_periods(periods)
    else:
        # The fallback exists so the job is usable by hand, and it says which range it
        # used so nobody mistakes it for the one they asked for.
        span = entries.agg(
            F.min("accounting_period").alias("first"),
            F.max("accounting_period").alias("last"),
        ).collect()[0]
        first, last = span["first"], span["last"]
        print(f"no --periods given; using the fact table's span {first}:{last}")

    # Explode first, format second: Spark refuses a generator nested inside an
    # expression.
    grid_periods = spark.sql(
        "SELECT explode(sequence("
        f"  to_date('{first}-01'), to_date('{last}-01'), interval 1 month"
        ")) AS month_start"
    ).select(F.date_format("month_start", "yyyy-MM").alias("accounting_period"))

    # One row per combination that has activity anywhere, times every period. The
    # account type travels with the combination: it is the type the entries carried,
    # which is the point-in-time one.
    identity = entries.select("account_code", "cost_center_code").distinct()
    grid = identity.crossJoin(grid_periods)

    posted = entries.groupBy("account_code", "cost_center_code", "accounting_period").agg(
        F.sum("amount_dr_base").cast(TOTAL).alias("debit_total"),
        F.sum("amount_cr_base").cast(TOTAL).alias("credit_total"),
        # One type per period, and it is the one in force at the period's close. An
        # account reclassified mid-month has entries carrying both types; keeping both
        # would match the same totals twice, duplicating the row and signing the two
        # copies against each other. A month is reported as of its end.
        F.max(F.struct("accounting_date", "account_type"))["account_type"].alias(
            "closing_type"
        ),
    )

    # A period with no entries has no account type of its own, so it borrows the one
    # from the nearest period that had entries. Without it the sign of a zero row would
    # be undefined - and a zero is signed the same either way, but the column should
    # still say what the account was.
    typed = Window.partitionBy("account_code", "cost_center_code").orderBy("accounting_period")
    dense = (
        grid.join(posted, ["account_code", "cost_center_code", "accounting_period"], "left")
        .withColumn("debit_total", F.coalesce("debit_total", F.lit(0).cast(TOTAL)))
        .withColumn("credit_total", F.coalesce("credit_total", F.lit(0).cast(TOTAL)))
        .withColumn(
            "account_type",
            F.coalesce(
                F.last("closing_type", ignorenulls=True).over(
                    typed.rowsBetween(Window.unboundedPreceding, 0)
                ),
                F.first("closing_type", ignorenulls=True).over(
                    typed.rowsBetween(0, Window.unboundedFollowing)
                ),
            ),
        )
        .withColumn(
            "balance",
            F.when(F.col("account_type").isin(*DEBIT_NORMAL),
                   F.col("debit_total") - F.col("credit_total"))
             .otherwise(F.col("credit_total") - F.col("debit_total"))
             .cast(TOTAL),
        )
    )

    ordered = Window.partitionBy("account_code", "cost_center_code").orderBy("accounting_period")

    def comparison(lag: int, suffix: str):
        """A delta that is always defined, and a percentage that is not.

        The percentage is null when the base is zero, which is exactly the case an
        anomaly investigation cares about most - an account that had nothing and now
        has something. The denominator is an absolute value: without it a balance
        moving from -100 to -50 divides by a negative and reports as a fall.
        """
        before = F.lag("balance", lag).over(ordered)
        delta = (F.col("balance") - before).cast(TOTAL)
        pct = F.when(before == 0, F.lit(None)).otherwise(delta / F.abs(before)).cast(RATIO)
        return delta.alias(f"balance_delta_{suffix}"), pct.alias(f"balance_pct_{suffix}")

    rolling = ordered.rowsBetween(-2, 0)
    result = dense.select(
        "account_code", "cost_center_code", "accounting_period", "account_type",
        "debit_total", "credit_total", "balance",
        *comparison(1, "mom"), *comparison(12, "yoy"),
        F.round(F.avg("balance").over(rolling), ROLLING_PLACES)
         .cast(TOTAL).alias("balance_rolling_3m"),
        F.count("balance").over(rolling).cast("int").alias("rolling_periods"),
    )

    target = staging_path(staging_dir)
    result.coalesce(1).write.mode("overwrite").parquet(str(target))
    return target


def frame(spark, staging_dir):
    return spark.read.parquet(str(staging_path(staging_dir)))


def read(spark, staging_dir) -> list[dict]:
    return [row.asDict() for row in frame(spark, staging_dir).collect()]


def checksum(rows) -> str:
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

    parser = argparse.ArgumentParser(prog="transform.spark.balances", description=__doc__)
    parser.add_argument("--staging", default="data/staging")
    parser.add_argument("--periods", help="FROM:TO, as YYYY-MM:YYYY-MM")
    args = parser.parse_args(argv)

    borrowed = session.active() is not None
    spark = session.build("fin-pipeline-balances")
    try:
        target = build(spark, args.staging, periods=args.periods)
        print(f"{MODEL} -> {target}")
    finally:
        if not borrowed:
            spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
