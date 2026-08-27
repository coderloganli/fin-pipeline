"""Synthetic general-ledger data.

The generator is a deliverable in its own right: every later test plants a specific
failure with it and asserts the pipeline responds correctly, and the golden set that
grades the insight layer has known answers only because the anomalies were put there
on purpose.

    from generator import Config, generate
    generate(Config(seed=42, out_dir=Path("data/source"), late_entries=True))

Given the same seed and configuration, two runs produce byte-identical files. Each
failure mode draws from its own random stream, so switching one on leaves the data
belonging to the others where it was.

See docs/adr/0005-deterministic-generation.md and
docs/adr/0006-generator-writes-streaming-csv.md.
"""

from . import dimensions, entries, schema
from .config import SCHEMA_DRIFT_CHOICES, Config
from .writers import TableWriter, columns_for

__all__ = ["Config", "generate"]

def _write(config: Config, table: str, rows) -> None:
    """One table, written a row at a time so nothing accumulates."""
    columns = columns_for(table, config.schema_drift, config.schema_drift_table)
    added = set(columns) - set(schema.COLUMNS[table])
    with TableWriter(config.out_dir, table, columns) as writer:
        for row in rows:
            # Drift moves the header and the rows together; a header-only change
            # would just be malformed CSV.
            for column in added:
                row.setdefault(column, schema.DRIFT_VALUE)
            writer.write(row)


def generate(config: Config) -> None:
    """Write the five source tables to `config.out_dir`."""
    months = entries.months_in(config.periods)

    account_rows = dimensions.accounts(config.seed)
    centre_rows = dimensions.cost_centres(config.seed, config.cost_centre_move)

    # The two accounts the growth anomaly uses exist in the dimension whether or not
    # the switch is on, so turning it on does not change dim_account_src.
    ordinary_accounts = [
        row["account_code"]
        for row in account_rows
        if row["account_code"] not in entries.RESERVED_ACCOUNTS
    ]
    centre_codes = sorted({row["cc_code"] for row in centre_rows})

    _write(config, "dim_account_src", iter(account_rows))
    _write(config, "dim_cost_center_src", iter(centre_rows))
    _write(config, "fx_rate", iter(dimensions.fx_rates(config.seed, months)))

    def all_entries():
        yield from entries.ordinary(config, months, ordinary_accounts, centre_codes)
        yield from entries.planted(config, months, ordinary_accounts, centre_codes)

    _write(config, "gl_entry", all_entries())

    entry_count = len(months) * max(1, config.entries_per_period // 2) * 2
    _write(
        config,
        "gl_adjustment",
        entries.adjustments(config, months, ordinary_accounts, centre_codes, entry_count),
    )
