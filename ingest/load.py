"""The watermarked incremental load, and the merge that makes a rerun free.

A run reads the source rows at or above the stored watermark less an overlap window,
applies them to the accounting periods they touch by primary key, and writes the
watermark only once every partition has been written. The order is the design:

1. Read the stored watermark W. `--full`, or no W yet, means no lower bound.
2. Stream the source CSV, keeping rows whose watermark column is at or above the
   bound. Inside one batch a repeated primary key collapses to the last occurrence.
3. Group what was kept by accounting period.
4. Merge each touched period - and only those. A table declaring no watermark is
   replaced whole instead.
5. Only now, store max(W, the highest watermark value in the batch).

A crash in step 4 leaves the watermark where it was, so the next run re-reads the same
window and converges; applying the same rows again is what a merge does. A watermark
advanced before the write would turn an interrupted run into permanently missing rows,
and that is the failure that does not announce itself.

An update that arrives after the window has passed is missed. That is what a
watermarked load is rather than a defect, and `--full` is what recovers from it - it
reaches the same raw layer because it goes through the same merge.

This does not re-run contract validation. `python -m ingest.validate` is the gate; the
DAG runs the two in order. See docs/adr/0014 and 0016.
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from . import contracts, raw, runs, validate

__all__ = [
    "DEFAULT_OVERLAP_DAYS",
    "digest_of",
    "Watermarks",
    "TableLoad",
    "LoadReport",
    "LoadError",
    "SourceShapeError",
    "StateError",
    "select_rows",
    "merge_partition",
    "load_table",
    "load_source",
    "main",
]

LOGGER = logging.getLogger("ingest.load")


class LoadError(ValueError):
    """The load cannot proceed. Never a bad value in a row - that is the validator's
    business, and `python -m ingest.validate` is the gate that reports it. These are
    the shapes that would otherwise make a run do damage quietly: a source the load
    cannot read, or a state file it cannot trust."""


class SourceShapeError(LoadError):
    """The source file is not the shape the load needs."""


class StateError(LoadError):
    """The stored watermark is not something the load can compare against."""

DEFAULT_SOURCE = Path("data/source")
DEFAULT_RAW = Path("data/raw")

# Seven days is a starting value, not a measured one: it has to exceed the largest gap
# between a row becoming visible in the source and the load running, and - because the
# watermark is a date - also absorb same-day updates arriving after a run. The right
# number is a property of the source system, which is why the flag exists.
DEFAULT_OVERLAP_DAYS = 7

STATE_DIR = "_state"
STATE_FILE = "watermarks.json"

NONE = "none"


# --- the watermark ---------------------------------------------------------

class Watermarks:
    """Where each table got to last time, kept beside the raw layer it describes."""

    def __init__(self, path: Path, values: dict[str, str]):
        self.path = path
        self.values = values

    @classmethod
    def load(cls, raw_dir) -> "Watermarks":
        path = Path(raw_dir) / STATE_DIR / STATE_FILE
        values = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
        # Checked on the way in, before anything has been written. A shape or a value
        # that cannot be used raises much later otherwise - a list reaches `.items()`
        # as an AttributeError, and a value that is not a date reaches strptime
        # halfway through a run.
        if not isinstance(values, dict):
            raise StateError(
                f"{path}: expected an object mapping each table to its watermark, "
                f"found {type(values).__name__}. Delete the file to reload from the "
                f"beginning."
            )
        for table, value in values.items():
            if not isinstance(value, str):
                raise StateError(f"{path}: the watermark for {table} is {value!r}, not a date")
            # The validator's own primitive rather than a second copy of the rule
            # (docs/adr/0010). strptime alone accepts "2026-2-01", which parses fine
            # and then sorts after "2026-10-01" - so the watermark would silently stop
            # advancing, which is the failure this whole class exists to prevent.
            problem = validate.check_value({"type": "date"}, value)
            if problem:
                raise StateError(
                    f"{path}: the watermark for {table} is unusable - {problem}. "
                    f"Delete the file to reload from the beginning."
                )
        return cls(path, values)

    def get(self, table: str) -> str | None:
        return self.values.get(table)

    def advance(self, table: str, value: str) -> None:
        """Forward only. A watermark that followed the batch rather than the maximum
        would step back whenever a run happened to see only older rows."""
        current = self.values.get(table)
        if current is None or value > current:
            self.values[table] = value

    def save(self) -> None:
        """Write the state file the way a partition is written: to a temporary file
        beside it, then moved over it.

        A half-written state file is worse than a missing one. Missing means "start
        again", which is slow and correct; half-written means the next run raises on a
        file nobody edited, and the operator has to reconstruct a number by hand.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.tmp")
        try:
            temporary.write_text(
                json.dumps(self.values, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)


def non_negative_days(text: str) -> int:
    """An overlap window argparse will not accept a negative value for.

    A negative window moves the lower bound *past* the watermark, so the run skips the
    rows nearest it - the exact ones the window exists to re-read. Zero is meaningful
    and allowed: it reads from the watermark forward and nothing before it.
    """
    try:
        days = int(text)
    except ValueError:
        # argparse would turn a bare ValueError into its generic "invalid value"
        # message. Raising the type it asks for keeps the reason in the output.
        raise argparse.ArgumentTypeError(f"{text!r} is not a whole number of days") from None
    if days < 0:
        raise argparse.ArgumentTypeError(
            f"{days} would move the lower bound past the watermark and skip the rows "
            f"nearest it; the window cannot be negative"
        )
    return days


def lower_bound(watermark: str | None, overlap_days: int) -> str | None:
    """How far back this run reads. None means the whole source."""
    if watermark is None:
        return None
    if overlap_days < 0:
        raise ValueError(f"the overlap window cannot be negative, got {overlap_days}")
    moment = datetime.strptime(watermark, contracts.DATE_FORMAT) - timedelta(days=overlap_days)
    return moment.strftime(contracts.DATE_FORMAT)


# --- what a run produced ---------------------------------------------------

@dataclass
class TableLoad:
    """What one table's load did. The CLI's summary line is rendered from these."""

    table: str
    source_sha256: str = ""
    rows_scanned: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_evicted: int = 0
    partitions_written: int = 0
    watermark_from: str | None = None
    watermark_to: str | None = None

    def describe(self) -> str:
        return (
            f"{self.table}: scanned {self.rows_scanned}, "
            f"inserted {self.rows_inserted}, updated {self.rows_updated}, "
            f"evicted {self.rows_evicted}, partitions {self.partitions_written}, "
            f"watermark {self.watermark_from or NONE} -> {self.watermark_to or NONE}"
        )


@dataclass
class LoadReport:
    tables: list[TableLoad] = field(default_factory=list)
    run_id: str = ""

    def describe(self) -> str:
        return "\n".join(table.describe() for table in self.tables)

    def as_table_runs(self) -> list[runs.TableRun]:
        """The report as the run record holds it. The two say the same thing about the
        same run, so neither gets to drift from the other."""
        return [
            runs.TableRun(
                table=table.table,
                source_sha256=table.source_sha256,
                rows_scanned=table.rows_scanned,
                rows_inserted=table.rows_inserted,
                rows_updated=table.rows_updated,
                rows_evicted=table.rows_evicted,
                partitions_written=table.partitions_written,
                watermark_from=table.watermark_from,
                watermark_to=table.watermark_to,
            )
            for table in self.tables
        ]


def digest_of(path) -> str:
    """The SHA-256 of a source file, read a megabyte at a time.

    A second sequential pass over a file the load is about to stream anyway. It is the
    only answer to whether the extract that was ingested is the bytes that were sent,
    and it is scoped to the run that read it rather than stamped onto rows a later
    rewrite may mix with others. See docs/adr/0020.
    """
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


# --- the walk --------------------------------------------------------------

def select_rows(contract: dict, path, bound: str | None):
    """Stream a source file, yielding the rows this run is responsible for.

    Rows stream and are yielded one at a time; what the caller then holds is the batch,
    which the window bounds. Nothing here holds the file.

    An added column is a compatible change, so a source is free to grow one called
    `_first_run_id`. It does not get to be one: the run identifiers are ingest's, they
    are written by `raw.write_partition` and by nothing else, and a source column of
    that name is dropped here rather than being carried into the batch and stamped onto
    a row as though raw had said it. See docs/adr/0018.
    """
    path = Path(path)
    column = contract.get("watermark")
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            # Not zero rows. A table loaded in full would mirror this into an empty
            # partition, so a truncated extract would erase what raw already held. A
            # file that genuinely carries no rows still carries its header.
            raise SourceShapeError(
                f"{contract['table']}: {path} is empty - it does not even carry a "
                f"header row, so it is a truncated extract rather than an empty one"
            )
        absent = [spec["name"] for spec in contract["columns"] if spec["name"] not in header]
        if absent:
            # Every declared column, not only the watermark one. A missing watermark
            # column would read as "" on every row - below any bound, so a second run
            # would report zero rows selected and an extract that lost a column would
            # look like an extract with no news in it. A missing key or partition
            # column escapes as a KeyError from somewhere further in, after part of the
            # batch may already be on disk. `python -m ingest.validate` is the gate that
            # calls a dropped column incompatible; this only refuses to guess.
            raise SourceShapeError(
                f"{contract['table']}: {path} does not carry {absent}, which the "
                f"contract declares; run `python -m ingest.validate` to see what changed"
            )
        for number, values in enumerate(reader, start=1):
            if len(values) != len(header):
                # zip would drop the columns past the short row's end, and the failure
                # would surface as a KeyError while writing a partition - by which time
                # earlier partitions of the same batch are already on disk.
                raise SourceShapeError(
                    f"{contract['table']}: {path} row {number} has {len(values)} "
                    f"fields, but the header declares {len(header)}"
                )
            row = {name: value for name, value in zip(header, values)
                   if name not in raw.METADATA}
            if bound is None or column is None or row[column] >= bound:
                yield row


def merge_partition(contract: dict, path, incoming, *, run_id: str) -> tuple[int, int]:
    """Apply `incoming` to one partition by primary key. Returns (inserted, updated).

    The incoming row wins, which is what makes the result a function of the union
    rather than of the order the writes happened in - and therefore what makes a rerun
    reach the same file.

    The partition is read with its run identifiers, and an incoming row that replaces a
    key already here inherits that key's `_first_run_id`: the run that brought the key
    is not the run that restated it. Rows the batch never mentions are carried across
    with theirs intact, which is what stops a partition reopened for one row from
    claiming this run landed all of them. See docs/adr/0018.
    """
    key_columns = contract["primary_key"]

    def key(row):
        return tuple(row[name] for name in key_columns)

    existing = {key(row): row for row in raw.read_partition(contract, path, metadata=True)}
    held = set(existing)
    # Counted over the keys the batch carries, not the rows: two incoming rows for one
    # key produce one row, and reporting two insertions would make the summary line
    # disagree with the partition it describes. `load_table` already collapses a
    # repeated key before it gets here, and this does not depend on that.
    for row in incoming:
        landed = key(row)
        first = existing.get(landed, {}).get(raw.FIRST_RUN_ID)
        existing[landed] = {**row, raw.FIRST_RUN_ID: first} if first else dict(row)
    touched = {key(row) for row in incoming}
    updated = len(touched & held)
    inserted = len(touched) - updated

    raw.write_partition(contract, path, list(existing.values()), run_id=run_id)
    return inserted, updated


def evict_moved_keys(contract: dict, raw_dir, home: dict[tuple[str, ...], str],
                     *, run_id: str) -> tuple[int, int]:
    """Remove each key in `home` from every partition except the one it now belongs to.

    A row's partition comes from its accounting date, which is not part of its primary
    key. So a correction that re-dates an entry into another period without bumping its
    version lands in a new partition while the old one still holds the same key, and raw
    would carry two rows for one key - the exact thing `(entry_id, version)` is there to
    prevent.

    Every partition is examined, including the ones the batch just merged: a period can
    be touched by one key while holding the stale copy of another. What decides is where
    each key belongs now, not whether the batch happened to write that period.

    Only the key columns of a partition are read; Parquet is columnar, so this does not
    pay for the rest of the row, and a partition holding none of the moved keys is not
    rewritten. Returns (rows evicted, partitions rewritten).
    """
    key_columns = contract["primary_key"]
    evicted = rewritten = 0

    for path in raw.partitions(contract, raw_dir):
        period = path.parent.name.split("=", 1)[1]
        moved = {
            key for key in raw.read_keys(contract, path)
            if key in home and home[key] != period
        }
        if not moved:
            continue

        rows = raw.read_partition(contract, path, metadata=True)
        kept = [row for row in rows if tuple(row[name] for name in key_columns) not in moved]
        # Rows, not keys. A partition already holding two rows for one key is exactly
        # the state this repairs, so saying "1" would understate what it did.
        evicted += len(rows) - len(kept)
        rewritten += 1
        if kept:
            raw.write_partition(contract, path, kept, run_id=run_id)
        else:
            # The period emptied out. A zero-row file would make the layout claim a
            # period exists that holds nothing.
            path.unlink()
            path.parent.rmdir()

    return evicted, rewritten


def load_table(
    contract: dict,
    source_dir,
    raw_dir,
    *,
    overlap_days: int = DEFAULT_OVERLAP_DAYS,
    full: bool = False,
    watermarks: Watermarks,
    run_id: str,
) -> TableLoad:
    """Load one source table into the raw layer."""
    table = contract["table"]
    source = Path(source_dir) / f"{table}.csv"
    if not source.is_file():
        # Not zero rows. A table loaded in full would replace its partition with
        # nothing, so an extract that failed to arrive would read as an extract saying
        # everything had been deleted, and the raw layer would be gone before anyone
        # asked why the file was missing.
        raise FileNotFoundError(f"no source extract for {table} at {source}")

    watermark_column = contract.get("watermark")
    stored = watermarks.get(table)
    result = TableLoad(
        table=table,
        source_sha256=digest_of(source),
        watermark_from=stored,
        watermark_to=stored,
    )

    bound = None if (full or watermark_column is None) else lower_bound(stored, overlap_days)

    # The batch, de-duplicated by primary key with the last occurrence winning. The
    # validator is what reports that the source repeated a key; this must not turn one
    # into two rows.
    key_columns = contract["primary_key"]
    batch: dict[tuple[str, ...], dict[str, str]] = {}
    highest = None
    for row in select_rows(contract, source, bound):
        result.rows_scanned += 1
        batch[tuple(row[name] for name in key_columns)] = row
        if watermark_column is not None:
            value = row[watermark_column]
            if highest is None or value > highest:
                highest = value

    if watermark_column is None:
        raw.write_table(contract, raw_dir, list(batch.values()), run_id=run_id)
        result.rows_inserted = len(batch)
        result.partitions_written = 1
        return result

    if not batch:
        return result

    # Which run first landed each key this table already holds, looked up before the
    # batch is grouped. `merge_partition` reads the partition it is about to write, so
    # it recovers the identifier for every key already in that partition - but a row
    # whose accounting date moved arrives in a partition that has never seen it, and
    # the copy carrying its identifier is in the partition it is about to be evicted
    # from. This is the same table-wide scan of key columns that `evict_moved_keys`
    # already makes on every run, one column wider. See docs/adr/0018.
    prior = raw.prior_first_run_ids(contract, raw_dir)

    grouped: dict[str, list[dict[str, str]]] = {}
    for key, row in batch.items():
        first = prior.get(key)
        if first:
            row = {**row, raw.FIRST_RUN_ID: first}
        grouped.setdefault(raw.partition_of(row[contract["partition_by"]]), []).append(row)

    for period, rows in sorted(grouped.items()):
        inserted, updated = merge_partition(
            contract, raw.partition_path(raw_dir, contract, period), rows, run_id=run_id
        )
        result.rows_inserted += inserted
        result.rows_updated += updated
        result.partitions_written += 1

    home = {
        key: raw.partition_of(row[contract["partition_by"]]) for key, row in batch.items()
    }
    evicted, rewritten = evict_moved_keys(contract, raw_dir, home, run_id=run_id)
    result.rows_evicted = evicted
    result.partitions_written += rewritten

    # Last, and only now: every partition is on disk.
    if highest is not None:
        watermarks.advance(table, highest)
        result.watermark_to = watermarks.get(table)
    watermarks.save()
    return result


def load_source(
    source_dir,
    raw_dir,
    tables: list[str] | None = None,
    *,
    overlap_days: int = DEFAULT_OVERLAP_DAYS,
    full: bool = False,
) -> LoadReport:
    """Load a source directory into a raw layer, and record that the run happened.

    Raises only for what is not about the data - a `source_dir` that is not a
    directory, a contract that does not load.

    The run record is opened before any table is read and closed whatever happens: a
    failure is written down, naming the table it happened on, and then re-raised
    unchanged, so the exit code is decided exactly where it was before. A record
    written only on the paths that worked would be missing from the run somebody is
    investigating. See docs/adr/0019.
    """
    source = Path(source_dir)
    if not source.is_dir():
        # Before the run opens: there was nothing here to run against, and a record
        # saying a run started would be a record of something that did not happen.
        raise NotADirectoryError(f"no source directory at {source}")

    names = list(tables) if tables is not None else contracts.tables()
    run_id = runs.new_run_id()
    log = runs.RunLog(raw_dir)
    log.start(run_id, command="load", source=str(source), raw=str(Path(raw_dir)),
              tables=names, overlap_days=overlap_days, full=full)

    started = time.monotonic()
    report = LoadReport(run_id=run_id)
    failed_table = None
    try:
        watermarks = Watermarks.load(raw_dir)
        for name in names:
            failed_table = name
            report.tables.append(load_table(
                contracts.load(name), source, raw_dir,
                overlap_days=overlap_days, full=full, watermarks=watermarks,
                run_id=run_id,
            ))
            failed_table = None
        watermarks.save()
    except Exception as failure:
        log.finish(run_id, status=runs.FAILED, duration_seconds=time.monotonic() - started,
                   tables=report.as_table_runs(), failed_table=failed_table,
                   error=f"{type(failure).__name__}: {failure}")
        raise

    log.finish(run_id, status=runs.SUCCEEDED, duration_seconds=time.monotonic() - started,
               tables=report.as_table_runs())
    return report


# --- the command -----------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ingest.load",
        description="Land source extracts into the raw layer. Entry tables are merged "
                    "on their primary key from a watermark with an overlap window; a "
                    "table declaring no watermark is replaced whole.",
    )
    parser.add_argument("--source", default=str(DEFAULT_SOURCE),
                        help="directory holding the source CSVs (default: data/source)")
    parser.add_argument("--raw", default=str(DEFAULT_RAW),
                        help="directory the raw layer lives in (default: data/raw)")
    parser.add_argument("--table", action="append", dest="tables", default=None,
                        help="load only this table; repeatable")
    parser.add_argument("--overlap-days", type=non_negative_days, default=DEFAULT_OVERLAP_DAYS,
                        help=f"how far back of the watermark to re-read "
                             f"(default: {DEFAULT_OVERLAP_DAYS})")
    parser.add_argument("--full", action="store_true",
                        help="ignore the stored watermark and re-read the whole source")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Turn a load into an exit code. The only place that knows about exit codes."""
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as ended:
        return int(ended.code or 0)

    # The application configures logging; the library never does.
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING, format="%(message)s")

    try:
        report = load_source(
            args.source, args.raw, tables=args.tables,
            overlap_days=args.overlap_days, full=args.full,
        )
    except (NotADirectoryError, FileNotFoundError, LoadError) as failure:
        print(f"{failure}", file=sys.stderr)
        return 2
    except contracts.ContractError as failure:
        print(f"{failure}", file=sys.stderr)
        return 2
    except json.JSONDecodeError as failure:
        # The library raises rather than treating a corrupt state file as "no
        # watermark", which would silently re-scan everything and report success. The
        # command still owes its caller an exit code.
        print(
            f"the watermark state file is not readable JSON: {failure}. "
            f"Delete it to reload from the beginning.",
            file=sys.stderr,
        )
        return 2

    print(f"run {report.run_id}")
    print(report.describe())
    return 0


if __name__ == "__main__":
    sys.exit(main())
