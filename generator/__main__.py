"""Command line entry point.

    python -m generator --seed 42 --periods 2026-01:2026-12                         --entries-per-period 100000 --out data/source

Step four drives scale from here, so row count, date range and output directory are
all parameters rather than constants.
"""

import argparse
from pathlib import Path

from . import SCHEMA_DRIFT_CHOICES, Config, generate

SWITCHES = (
    "late_entries",
    "restatements",
    "cost_centre_move",
    "unbalanced_vouchers",
    "growing_account",
    "amount_outliers",
)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="generator", description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--periods", default="2026-01:2026-12", help="YYYY-MM:YYYY-MM")
    parser.add_argument("--entries-per-period", type=int, default=1_000)
    parser.add_argument("--out", type=Path, default=Path("data/source"))
    parser.add_argument("--schema-drift", choices=SCHEMA_DRIFT_CHOICES, default="none")
    parser.add_argument("--schema-drift-table", default="gl_entry")
    for switch in SWITCHES:
        parser.add_argument(
            f"--enable-{switch.replace('_', '-')}",
            dest=switch,
            action="store_true",
            help=f"plant the {switch.replace('_', ' ')} failure mode",
        )

    args = parser.parse_args(argv)
    config = Config(
        seed=args.seed,
        out_dir=args.out,
        periods=args.periods,
        entries_per_period=args.entries_per_period,
        schema_drift=args.schema_drift,
        schema_drift_table=args.schema_drift_table,
        **{switch: getattr(args, switch) for switch in SWITCHES},
    )
    generate(config)
    print(f"wrote {len(list(config.out_dir.glob('*.csv')))} tables to {config.out_dir}")


if __name__ == "__main__":
    main()
