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

__all__ = ["MODEL", "DEBIT_NORMAL", "MOM_LAG", "YOY_LAG", "ROLLING_SPAN",
           "dirty_closure", "build", "read", "frame", "checksum", "main"]

MODEL = "agg_monthly_balance"

# The windows the comparison columns are computed over. Constants rather than literals
# in `build`, because `dirty_closure` reads them: a dirty period drags the periods whose
# columns are computed through it, and writing that set down a second time beside the
# windows is how a widened window quietly stops being backfilled. See docs/adr/0040.
MOM_LAG = 1
YOY_LAG = 12
ROLLING_SPAN = 3

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


def shift(period: str, months: int) -> str:
    """`2026-11` and 3 gives `2027-02`."""
    year, month = (int(part) for part in period.split("-"))
    index = (year * 12 + month - 1) + months
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def dirty_closure(periods, *, last_period: str) -> set[str]:
    """The periods a set of dirty periods drags with it.

    Forwards only, and read off the window constants rather than restated: a dirty
    period M changes M's own balance, M+MOM_LAG's month-on-month comparison, the
    rolling mean of every period the span still reaches, and M+YOY_LAG's year-on-year
    one. Widening a window therefore widens this, which is the property that stops a
    backfill quietly skipping periods the aggregate had changed.

    Nothing reaches backwards. No column of M-1's row reads forward; the one expression
    that does is the account type borrowed into a period that posted nothing, and that
    is covered by reading the whole fact rather than by widening this. See docs/adr/0040
    and 0041.
    """
    closed: set[str] = set()
    for period in periods:
        candidates = [period, shift(period, MOM_LAG), shift(period, YOY_LAG)]
        candidates += [shift(period, step) for step in range(1, ROLLING_SPAN)]
        closed.update(p for p in candidates if p <= last_period)
    return closed


def build(spark, staging_dir, periods: str | None = None, dirty=None) -> Path:
    """Aggregate the fact tables into monthly balances over a dense grid.

    `dirty` scopes what is written, not what is read. The grid's membership and the type
    carried into a period that posted nothing are properties of the whole fact table, so
    a build that read only the dirty periods would answer differently for them - see
    docs/adr/0041. None writes every period, which is what a full run does.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    entries = facts.frame(spark, staging_dir)
    adjustments = adjustment_frame(spark, staging_dir)

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
    #
    # `anywhere` is the whole fact table, and deliberately: density is a statement about
    # every period, so a membership set derived from a window would produce a grid with
    # holes in it. Corrections count towards membership - they are postings on the
    # as-reported basis - and so do restatements, because a combination that exists only
    # on the restated basis still needs a row. See docs/adr/0033 and 0041.
    identity = (
        entries.select("account_code", "cost_center_code")
        .union(adjustments.select("account_code", "cost_center_code"))
        .distinct()
    )
    grid = identity.crossJoin(grid_periods)

    # The as-reported basis: entries and `correction` adjustments. A correction amends
    # the period's figure, so after it lands there is one number and it is the amended
    # one. See docs/adr/0043.
    reported = entries.select(
        "account_code", "cost_center_code", "accounting_period",
        "accounting_date", "account_type", "amount_dr_base", "amount_cr_base",
    ).union(
        adjustments.where(F.col("adjustment_type") == "correction").select(
            "account_code", "cost_center_code", "accounting_period",
            "accounting_date", "account_type", "amount_dr_base", "amount_cr_base",
        )
    )

    posted = reported.groupBy("account_code", "cost_center_code", "accounting_period").agg(
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

    # The restatement side, kept apart. It is the bridge between the two bases and a
    # figure in its own right - the number an analyst asking "what changed" wants - and
    # it is the seam master data would join if docs/adr/0027 ever brought it into scope.
    restated = (
        adjustments.where(F.col("adjustment_type") == "restatement")
        .groupBy("account_code", "cost_center_code", "accounting_period")
        .agg(
            F.sum("amount_dr_base").cast(TOTAL).alias("restatement_debit"),
            F.sum("amount_cr_base").cast(TOTAL).alias("restatement_credit"),
        )
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
        .join(restated, ["account_code", "cost_center_code", "accounting_period"], "left")
        .withColumn("restatement_debit",
                    F.coalesce("restatement_debit", F.lit(0).cast(TOTAL)))
        .withColumn("restatement_credit",
                    F.coalesce("restatement_credit", F.lit(0).cast(TOTAL)))
    )

    def signed(debit, credit):
        """Debit-normal accounts grow on the debit side and credit-normal ones on the
        credit side, so every account's ordinary activity reads as a positive number
        that grows. The delta is signed the same way the balance is: it is a balance
        movement, not a raw debit-minus-credit. See docs/adr/0032."""
        return (
            F.when(F.col("account_type").isin(*DEBIT_NORMAL), F.col(debit) - F.col(credit))
             .otherwise(F.col(credit) - F.col(debit))
             .cast(TOTAL)
        )

    based = (
        dense
        .withColumn("balance_as_reported", signed("debit_total", "credit_total"))
        .withColumn("restatement_delta",
                    signed("restatement_debit", "restatement_credit"))
        .withColumn(
            "balance_as_restated",
            (F.col("balance_as_reported") + F.col("restatement_delta")).cast(TOTAL),
        )
    )

    ordered = Window.partitionBy("account_code", "cost_center_code").orderBy("accounting_period")

    def comparison(lag: int, suffix: str):
        """A delta that is always defined, and a percentage that is not.

        The percentage is null when the base is zero, which is exactly the case an
        anomaly investigation cares about most - an account that had nothing and now
        has something. The denominator is an absolute value: without it a balance
        moving from -100 to -50 divides by a negative and reports as a fall.

        Over the restated basis, because that is the answer a report shows: a
        month-on-month delta computed on a superseded basis flags movements that are
        artefacts of not having looked at the restatement. See docs/adr/0043.
        """
        before = F.lag("balance_as_restated", lag).over(ordered)
        delta = (F.col("balance_as_restated") - before).cast(TOTAL)
        pct = F.when(before == 0, F.lit(None)).otherwise(delta / F.abs(before)).cast(RATIO)
        return delta.alias(f"balance_delta_{suffix}"), pct.alias(f"balance_pct_{suffix}")

    rolling = ordered.rowsBetween(-(ROLLING_SPAN - 1), 0)
    result = based.select(
        "account_code", "cost_center_code", "accounting_period", "account_type",
        "debit_total", "credit_total",
        "balance_as_reported", "restatement_delta", "balance_as_restated",
        *comparison(MOM_LAG, "mom"), *comparison(YOY_LAG, "yoy"),
        F.round(F.avg("balance_as_restated").over(rolling), ROLLING_PLACES)
         .cast(TOTAL).alias(f"balance_rolling_{ROLLING_SPAN}m"),
        F.count("balance_as_restated").over(rolling).cast("int").alias("rolling_periods"),
    )

    return write_result(result, staging_dir, first=first, last=last, dirty=dirty)


def adjustment_frame(spark, staging_dir):
    """The adjustment fact, or an empty frame shaped like it.

    Empty rather than absent: the aggregate is defined whether or not any adjustment has
    ever been landed, and a build that raised on a ledger with no corrections would fail
    on the ordinary case.
    """
    from pyspark.sql import functions as F

    path = facts.staging_path(staging_dir, model=facts.ADJUSTMENT_MODEL)
    landed = path.is_dir() and any(path.rglob("*.parquet"))
    if landed:
        return facts.frame(spark, staging_dir, model=facts.ADJUSTMENT_MODEL)
    return (
        facts.frame(spark, staging_dir)
        .withColumn("adjustment_type", F.lit(None).cast("string"))
        .limit(0)
    )


def write_result(result, staging_dir, *, first: str, last: str, dirty=None) -> Path:
    """Write the closure's partitions, or every period.

    A change in the grid's membership discards the closure and rewrites everything. A
    combination seen for the first time needs a zero row in every period, and one whose
    last entry went away has a stale row in every period; writing only the closure would
    leave a grid with holes that no gate catches, because an absent row satisfies both
    `unique` and `not_null`. See docs/adr/0041.
    """
    every = set(period_range(first, last))
    stale = [period for period in partitions_held(staging_dir) if period not in every]

    if dirty is None or rebuild_everything(result, staging_dir, every):
        writing = every
    else:
        writing = dirty_closure(dirty, last_period=last) & every

    # A period outside the reporting range is removed whatever the write scope. Leaving
    # it would make the layer claim a period the range no longer covers, and no
    # comparison against the range would ever look at it again.
    return facts.write_partitions(
        result, staging_dir, model=MODEL, periods=set(writing) | set(stale)
    )


def partitions_held(staging_dir) -> list[str]:
    return facts.partitions(staging_dir, model=MODEL)


def rebuild_everything(result, staging_dir, every: set[str]) -> bool:
    """Whether the closure has to be discarded and the whole range rewritten.

    Three reasons, and they are the same reason: the dense grid is a statement about
    every period, so anything that changes what the grid should contain outside the
    dirty closure invalidates it. See docs/adr/0033 and 0041.

    **There is no aggregate yet.** A first build has nothing to be selective about, and
    writing only the closure would create a layer that was sparse from the start.

    **The reporting range moved.** A period newly inside the range has no rows at all,
    and no closure of a dirty period would reach it.

    **The grid's membership changed.** A combination seen for the first time needs a
    zero row in every period, and one whose last entry went away has a stale row in
    every period - holes no gate catches, because an absent row satisfies both `unique`
    and `not_null`.
    """
    root = staging_path(staging_dir)
    if not root.is_dir() or not any(root.rglob("*.parquet")):
        return True

    if set(partitions_held(staging_dir)) != every:
        return True

    spark = result.sparkSession
    held = spark.read.parquet(str(root)).select(
        "account_code", "cost_center_code").distinct()
    wanted = result.select("account_code", "cost_center_code").distinct()
    return not (
        held.subtract(wanted).isEmpty() and wanted.subtract(held).isEmpty()
    )


def period_range(first: str, last: str) -> list[str]:
    """Every period from `first` to `last` inclusive."""
    found, period = [], first
    while period <= last:
        found.append(period)
        period = shift(period, 1)
    return found


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

    with session.acquire("fin-pipeline-balances") as spark:
        target = build(spark, args.staging, periods=args.periods)
        print(f"{MODEL} -> {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
