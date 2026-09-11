"""One dirty-set-driven pass over the staging layer.

The order is the design. The dimensions are rebuilt first because resolving a dimension
version needs the validity intervals as they now stand; the versions are then resolved
against those and against the fact as it currently is; the facts are rebuilt for the
periods that changed; and the aggregate is rebuilt for the closure of those periods.

It does not clear the affected-period set. That is `python -m ingest.affected --clear`,
and it is the orchestrator's own step: a module that silently cleared shared state on
success cannot be run twice, or alone, without consequences that are not visible where
the command is typed. See docs/adr/0039.

    python -m transform.backfill --raw data/raw --staging data/staging \\
        --periods 2026-01:2026-12
"""

import argparse

from ingest import affected as affected_state
from ingest import contracts
from transform.spark import affected as resolve
from transform.spark import balances, facts, scd2

__all__ = ["run", "main"]


def run(spark, raw_dir, staging_dir, *, periods: str, force: bool = False) -> set[str]:
    """Rebuild what the affected-period set says is owed. Returns the periods written.

    `force` adds the whole requested range to whatever is owed. Without it, an empty
    affected set means nothing is rebuilt, which is right for a nightly run and wrong
    for a backfill: the reason to type a range by hand is precisely that the set does
    not name it - a bug that has been fixed, or the update that arrived after the
    watermark window closed, which docs/adr/0016 names as the case this recovers.
    """
    first, last = balances.parse_periods(periods)

    # Whole, and first. They are tens of rows - docs/adr/0026 keeps them rebuilt - and
    # the resolution below reads the intervals they produce.
    for table in sorted(scd2.MODELS):
        scd2.build(spark, contracts.load(table), raw_dir, staging_dir)

    owed = affected_state.read(raw_dir)
    if owed.is_empty() and not force:
        return set()

    dirty = resolve.resolve(spark, raw_dir, staging_dir, owed, last_period=last)
    if force:
        dirty = set(dirty) | set(balances.period_range(first, last))
    dirty = {period for period in dirty if first <= period <= last}
    if not dirty:
        return set()

    for model in sorted(facts.SOURCES):
        facts.build(spark, raw_dir, staging_dir, model=model, dirty=dirty)
    balances.build(spark, staging_dir, periods=periods, dirty=dirty)
    return dirty


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="transform.backfill", description=__doc__)
    parser.add_argument("--raw", default="data/raw")
    parser.add_argument("--staging", default="data/staging")
    parser.add_argument("--periods", required=True,
                        help="the reporting range, as YYYY-MM:YYYY-MM")
    parser.add_argument("--force", action="store_true",
                        help="rebuild the whole range, whether or not the "
                             "affected-period set names it")
    return parser


def main(argv: list[str] | None = None) -> int:
    from transform.spark import session

    args = build_parser().parse_args(argv)

    with session.acquire("fin-pipeline-backfill") as spark:
        written = run(spark, args.raw, args.staging, periods=args.periods,
                      force=args.force)

    # After the block, as it was after the `finally`: the session goes down, then the run
    # says what it rebuilt.
    if written:
        print(f"rebuilt {len(written)} periods: {', '.join(sorted(written))}")
    else:
        print("nothing owed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
