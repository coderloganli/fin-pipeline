"""Turning what ingest observed into the set of periods to recompute.

`ingest/affected.py` records observations and stops there: the periods a merge wrote,
and the effective-dated versions it inserted. Translating a version into periods needs
two things ingest does not have - the version's validity interval, which is a property
of the staged SCD2 dimension, and which periods carry entries on that natural key, which
is a property of the staging fact. Both belong here.

A version dirties the periods its interval covers, intersected with the periods that
actually carry entries on its key. No recomputation is scheduled for a period that has
nothing on that key: that is not an exception to the rule, it is the rule, and
`fx_rate` is included on the same footing as the two org dimensions because a new rate
changes every converted amount on its dates.

See docs/adr/0039 and 0027.
"""

from ingest import affected as state
from ingest import contracts
from transform.spark import balances, facts, scd2

__all__ = ["FACT_COLUMNS", "periods_for_version", "resolve"]

# Which fact column each dimension's natural key appears as. The rate joins on the
# currency an entry was posted in; the two org dimensions join on their own code, and
# the cost centre's differs between the source (`cc_code`) and the fact
# (`cost_center_code`), which is why this is declared rather than derived from
# `scd2.MODELS`.
FACT_COLUMNS = {
    "dim_account_src": "account_code",
    "dim_cost_center_src": "cost_center_code",
    "fx_rate": "currency",
}


def periods_for_version(spark, staging_dir, version: dict, *, last_period: str) -> set[str]:
    """The periods one dimension version covers, intersected with activity on its key."""
    from pyspark.sql import functions as F

    table = version["table"]
    if table not in scd2.MODELS:
        raise ValueError(
            f"{table} is not an effective-dated dimension. "
            f"This module resolves {sorted(scd2.MODELS)}."
        )
    _, natural_key, _ = scd2.MODELS[table]
    contract = contracts.load(table)

    matching = scd2.frame(spark, contract, staging_dir)
    for column, value in zip(natural_key, version["key"]):
        matching = matching.where(F.col(column) == value)
    rows = matching.where(
        F.date_format("valid_from", "yyyy-MM-dd") == version["effective_date"]
    ).select("valid_from", "valid_to").collect()
    if not rows:
        return set()

    covered: set[str] = set()
    for row in rows:
        first = row["valid_from"].strftime("%Y-%m")
        # The version in force ends at the 9999-12-31 sentinel - docs/adr/0024 - and
        # that is a date, not a number of periods to recompute. It is cut at the end of
        # the reporting range.
        last = min(row["valid_to"].strftime("%Y-%m"), last_period)
        covered.update(balances.period_range(first, last))

    return covered & active_periods(spark, staging_dir, table, version["key"])


def active_periods(spark, staging_dir, table: str, key) -> set[str]:
    """The periods carrying entries on one dimension's natural key.

    The fact's own columns, read narrowly: this is the intersection that stops a
    dimension change scheduling recomputation for periods it could not have altered.
    """
    from pyspark.sql import functions as F

    if table not in FACT_COLUMNS:
        raise ValueError(
            f"{table} is an effective-dated dimension but no fact column is declared "
            f"for it. Add it to FACT_COLUMNS in this module: a dimension whose versions "
            f"cannot be intersected with activity would either recompute every period "
            f"or none, and both are wrong."
        )
    column = FACT_COLUMNS[table]

    found: set[str] = set()
    for model in (facts.MODEL, facts.ADJUSTMENT_MODEL):
        path = facts.staging_path(staging_dir, model=model)
        if not path.is_dir() or not any(path.rglob("*.parquet")):
            continue
        rows = (
            facts.frame(spark, staging_dir, model=model)
            .where(F.col(column) == key[0])
            .select(facts.PARTITION)
            .distinct()
            .collect()
        )
        found.update(row[facts.PARTITION] for row in rows)
    return found


def resolve(spark, raw_dir, staging_dir, affected: "state.Affected", *,
            last_period: str) -> set[str]:
    """Everything owed, as periods.

    Not closed under the aggregate's windows. The closure belongs to the aggregate that
    defines it - `balances.build` applies it to whatever it is given - and applying it
    here as well would widen the set twice over: a period made dirty only because it is
    three months after a real change would then drag its own three months with it, and
    a backfill of one period would walk forward through the year. The facts have no
    windows at all and want exactly this set. See docs/adr/0040.

    The dimension versions are resolved against the dimensions as they are now staged,
    so the caller has to have rebuilt them first - which `transform/backfill.py` does.
    """
    dirty = set(affected.periods)
    for version in affected.dimension_versions:
        dirty |= periods_for_version(spark, staging_dir, version,
                                     last_period=last_period)
    return {period for period in dirty if period <= last_period}
