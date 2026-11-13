"""Incremental CSV writing.

Rows are written as they arrive and never collected, so peak memory does not track
row count. Every byte-level detail is pinned rather than left to the platform,
because tests compare these files byte for byte and the machine this is developed on
is Windows, where csv's defaults end rows with CRLF.

See docs/adr/0006-generator-writes-streaming-csv.md.
"""

import csv
from pathlib import Path

from . import schema


class TableWriter:
    """Writes one table. Use as a context manager; rows go out one at a time."""

    def __init__(self, out_dir: Path, table: str, columns: tuple[str, ...] | None = None):
        self.path = out_dir / f"{table}.csv"
        self.columns = columns if columns is not None else schema.COLUMNS[table]
        self._handle = None
        self._writer = None

    def __enter__(self) -> "TableWriter":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # newline="" stops the csv module's terminator being translated again on the
        # way out; lineterminator makes that terminator LF on every platform.
        self._handle = self.path.open("w", encoding="utf-8", newline="")
        self._writer = csv.writer(self._handle, lineterminator="\n")
        self._writer.writerow(self.columns)
        return self

    def write(self, row: dict[str, object]) -> None:
        self._writer.writerow([row[column] for column in self.columns])

    def __exit__(self, *exc) -> None:
        self._handle.close()


def columns_for(table: str, drift: str, drift_table: str) -> tuple[str, ...]:
    """The columns a table is written with, after any schema drift.

    Drift changes the header and the rows together. A header-only change would
    produce malformed CSV, and ingest would fail parsing it — which is a different
    failure from the incompatible schema change this switch exists to exercise.
    """
    declared = schema.COLUMNS[table]
    if table != drift_table or drift == "none":
        return declared
    if drift == "add_column":
        return declared + (schema.DRIFT_COLUMN,)
    if drift == "drop_column":
        dropped = schema.DRIFT_DROP_COLUMN[table]
        return tuple(column for column in declared if column != dropped)
    raise ValueError(f"unknown schema_drift {drift!r}")
