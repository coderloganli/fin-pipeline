"""The run record: what a run did, written before the run is over.

`data/raw/_state/runs.jsonl` is appended to twice per run and never rewritten:

    {"event": "started",  "run_id": ..., "started_at": ..., "tables": [...], ...}
    {"event": "finished", "run_id": ..., "status": "succeeded", "tables": [...], ...}

The started event records the intent - this run, these tables, this window - and the
finished event records what came of it. What is written first is what survives the
failure, which is the same ordering argument the watermark is written with. A record
produced only at the end would be missing from exactly the run somebody is
investigating at eight in the morning; a run that died leaves its first line, and
`read` reports it as `interrupted`. A run that is genuinely still going looks
identical, and saying so is more honest than inventing a heartbeat.

Appending rather than rewriting means there is no window in which the file is half a
history, and no reader has to trust that the last writer finished.

Each table's row counts, watermark range and source file digest live here rather than
on the row: they are facts about a run, not about a row. From any raw row,
`_first_run_id` and `_last_run_id` reach the runs that landed and last wrote it, and
through them everything recorded here. See docs/adr/0019 and 0020.
"""

import argparse
import json
import os
import secrets
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

__all__ = [
    "STATE_DIR",
    "LOG_FILE",
    "SUCCEEDED",
    "FAILED",
    "INTERRUPTED",
    "RunLogError",
    "TableRun",
    "Run",
    "RunLog",
    "new_run_id",
    "now",
    "token",
    "main",
]

STATE_DIR = "_state"
LOG_FILE = "runs.jsonl"

DEFAULT_RAW = Path("data/raw")

SUCCEEDED = "succeeded"
FAILED = "failed"
INTERRUPTED = "interrupted"

DEFAULT_LIMIT = 10


class RunLogError(ValueError):
    """The log cannot be read. A line that will not parse is not skipped: a log that
    quietly drops what it cannot understand reports a history missing the run somebody
    is looking for, and does it without saying so."""


# --- the identifier --------------------------------------------------------

def now() -> datetime:
    """The wall clock, in UTC. A seam rather than a call to `datetime` at the point of
    use, so a test can fix the second two runs start in."""
    return datetime.now(timezone.utc)


def token() -> str:
    """The identifier's random half. Also a seam, for the same reason."""
    return secrets.token_hex(3)


def new_run_id() -> str:
    """`YYYYMMDDTHHMMSSZ-xxxxxx`.

    The timestamp orders the log to the second by eye, without parsing. The suffix
    keeps two runs started inside the same second apart - those two sort arbitrarily
    with respect to each other, which is why chronological order comes from the file
    and not from sorting these. See docs/adr/0019.
    """
    return f"{now().strftime('%Y%m%dT%H%M%SZ')}-{token()}"


# --- what is recorded ------------------------------------------------------

@dataclass
class TableRun:
    """What one table's load did, as the record holds it."""

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
            f"watermark {self.watermark_from or 'none'} -> {self.watermark_to or 'none'}"
        )


@dataclass
class Run:
    """One run, folded from its events."""

    run_id: str
    started_at: str
    status: str = INTERRUPTED
    finished_at: str | None = None
    duration_seconds: float | None = None
    command: str = ""
    source: str = ""
    raw: str = ""
    requested_tables: list[str] = field(default_factory=list)
    overlap_days: int | None = None
    full: bool = False
    tables: list[TableRun] = field(default_factory=list)
    failed_table: str | None = None
    error: str | None = None

    @property
    def rows_scanned(self) -> int:
        return sum(table.rows_scanned for table in self.tables)


# --- the log ---------------------------------------------------------------

class RunLog:
    """The append-only record of every run, kept beside the raw layer it describes."""

    def __init__(self, raw_dir):
        self.path = Path(raw_dir) / STATE_DIR / LOG_FILE

    def append(self, event: dict) -> None:
        """One line, flushed and fsynced before the call returns.

        A run record that is still in a buffer when the process dies is the record of
        the run that most needed one.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def start(self, run_id: str, *, command: str, source: str, raw: str,
              tables: list[str], overlap_days: int | None = None,
              full: bool = False) -> None:
        self.append({
            "event": "started",
            "run_id": run_id,
            "started_at": now().isoformat(),
            "command": command,
            "source": str(source),
            "raw": str(raw),
            "tables": list(tables),
            "overlap_days": overlap_days,
            "full": full,
        })

    def finish(self, run_id: str, *, status: str, duration_seconds: float,
               tables: list[TableRun], failed_table: str | None = None,
               error: str | None = None) -> None:
        if status not in {SUCCEEDED, FAILED}:
            # `interrupted` is what the absence of this event means, so writing it
            # would be a run claiming to have finished by not finishing.
            raise ValueError(
                f"a finished run is {SUCCEEDED} or {FAILED}, not {status!r}"
            )
        self.append({
            "event": "finished",
            "run_id": run_id,
            "finished_at": now().isoformat(),
            "duration_seconds": round(duration_seconds, 3),
            "status": status,
            "tables": [asdict(table) for table in tables],
            "failed_table": failed_table,
            "error": error,
        })

    def read(self) -> list[Run]:
        """Every run the log holds, in the order it was written.

        Not sorted: the file is the chronology, and two identifiers from the same
        second carry no order between them.
        """
        if not self.path.is_file():
            return []

        order: list[str] = []
        found: dict[str, Run] = {}
        for number, line in enumerate(
            self.path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if not line.strip():
                continue
            event = self._parse(line, number)
            kind = event.get("event")
            run_id = event.get("run_id")
            if not isinstance(run_id, str) or kind not in {"started", "finished"}:
                raise RunLogError(
                    f"{self.path} line {number}: not a run event - found "
                    f"{event.get('event')!r} for run {run_id!r}"
                )
            if kind == "started":
                if run_id in found:
                    # Two runs cannot share an identifier: six hex characters inside
                    # one second makes a collision negligible, so this means the log
                    # was concatenated or edited. Reading on would hand the caller a
                    # history in which one identifier means two different runs, which
                    # is worse than refusing to read it.
                    raise RunLogError(
                        f"{self.path} line {number}: run {run_id} starts twice"
                    )
                order.append(run_id)
                found[run_id] = Run(
                    run_id=run_id,
                    started_at=event.get("started_at", ""),
                    command=event.get("command", ""),
                    source=event.get("source", ""),
                    raw=event.get("raw", ""),
                    requested_tables=list(event.get("tables") or []),
                    overlap_days=event.get("overlap_days"),
                    full=bool(event.get("full")),
                )
                continue

            record = found.get(run_id)
            if record is not None and record.finished_at is not None:
                # The same reasoning as a repeated start: a run writes one finished
                # event, and a second one would silently replace the outcome of the
                # first. Which of the two is true is not something this can decide.
                raise RunLogError(
                    f"{self.path} line {number}: run {run_id} finishes twice"
                )
            if record is None:
                # A finished event with nothing that started it. The log is appended to
                # and never edited, so this means it was truncated or written by
                # something else - either way, reading past it would report a history
                # that is not what happened.
                raise RunLogError(
                    f"{self.path} line {number}: run {run_id} finished without ever "
                    f"having started"
                )
            record.status = event.get("status", INTERRUPTED)
            record.finished_at = event.get("finished_at")
            record.duration_seconds = event.get("duration_seconds")
            record.failed_table = event.get("failed_table")
            record.error = event.get("error")
            record.tables = [TableRun(**table) for table in event.get("tables") or []]

        return [found[run_id] for run_id in order]

    def _parse(self, line: str, number: int) -> dict:
        try:
            event = json.loads(line)
        except json.JSONDecodeError as failure:
            raise RunLogError(
                f"{self.path} line {number} is not readable JSON: {failure.msg}"
            ) from failure
        if not isinstance(event, dict):
            raise RunLogError(
                f"{self.path} line {number}: expected an object, found "
                f"{type(event).__name__}"
            )
        return event


# --- the command -----------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ingest.runs",
        description="Read the run record: what each run handled, and how long it took.",
    )
    parser.add_argument("--raw", default=str(DEFAULT_RAW),
                        help="directory the raw layer lives in (default: data/raw)")
    parser.add_argument("--run", dest="run_id", default=None,
                        help="show one run in full rather than listing the recent ones")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"how many runs to list (default: {DEFAULT_LIMIT})")
    return parser


def summarise(record: Run) -> str:
    duration = "-" if record.duration_seconds is None else f"{record.duration_seconds:.2f}s"
    return (
        f"{record.run_id}  {record.status:<11} {record.started_at}  "
        f"{duration:>10}  {len(record.tables)} tables  {record.rows_scanned} rows scanned"
    )


def detail(record: Run) -> str:
    lines = [
        f"run {record.run_id}",
        f"  command    {record.command}",
        f"  source     {record.source}",
        f"  raw        {record.raw}",
        f"  started    {record.started_at}",
        f"  finished   {record.finished_at or '-'}",
        f"  duration   {'-' if record.duration_seconds is None else f'{record.duration_seconds:.2f}s'}",
        f"  status     {record.status}",
    ]
    if record.full:
        lines.append("  full       yes")
    elif record.overlap_days is not None:
        lines.append(f"  overlap    {record.overlap_days} days")
    if record.failed_table:
        lines.append(f"  failed on  {record.failed_table}")
    if record.error:
        lines.append(f"  error      {record.error}")
    if not record.tables:
        lines.append("  no table was recorded for this run")
    for table in record.tables:
        lines.append(f"  {table.describe()}")
        lines.append(f"    source sha256 {table.source_sha256}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """Turn a read of the log into an exit code. The library raises; this is the only
    place that decides what a failure is worth. See docs/adr/0012."""
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as ended:
        return int(ended.code or 0)

    if args.limit < 1:
        print(f"--limit must be at least 1, got {args.limit}", file=sys.stderr)
        return 2

    try:
        records = RunLog(args.raw).read()
    except RunLogError as failure:
        print(f"{failure}", file=sys.stderr)
        return 2

    if args.run_id is not None:
        for record in records:
            if record.run_id == args.run_id:
                print(detail(record))
                return 0
        print(f"no run {args.run_id} in {RunLog(args.raw).path}", file=sys.stderr)
        return 2

    if not records:
        print(f"no runs recorded in {RunLog(args.raw).path}")
        return 0

    for record in reversed(records[-args.limit:]):
        print(summarise(record))
    return 0


if __name__ == "__main__":
    sys.exit(main())
