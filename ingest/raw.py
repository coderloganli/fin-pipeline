"""The raw layer: where a landed row lives, and what makes two runs comparable.

Raw holds the source as it arrived. Every column is written as a Parquet string and an
empty field stays an empty string. Unifying types is `staging`'s line in the layering
table, and a raw layer that already reinterpreted cannot answer the question it exists
for - whether the source really said that. See
docs/adr/0015-raw-lands-every-column-as-text.md.

Two columns are written beside the ones the contract declares, and they are the only
ones: `_first_run_id`, the run that first landed the key, and `_last_run_id`, the run
that last wrote the file. They belong to ingest rather than to the source, and they are
outside the checksum. See docs/adr/0018-raw-rows-carry-two-run-identifiers.md.

A table whose contract declares `partition_by` lands one file per accounting period:

    <raw_dir>/<table>/accounting_period=YYYY-MM/part-0000.parquet

A table that declares none lands one file at `<raw_dir>/<table>/part-0000.parquet`.
One file per partition rather than one per run: appending would make a rerun visible
in the layout even when the rows are identical, and the row count the acceptance
criterion reads would then depend on how many times the load ran.

Nothing here knows about watermarks or merging - that is `ingest.load`. What this
module owns is the layout, and the two measurements a rerun is judged by.
"""

import hashlib
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

__all__ = [
    "PART_FILE",
    "PARTITION_KEY",
    "METADATA",
    "prior_first_run_ids",
    "partition_of",
    "table_dir",
    "partition_path",
    "partitions",
    "read_keys",
    "read_partition",
    "write_partition",
    "write_table",
    "read_table",
    "row_count",
    "checksum",
]

PART_FILE = "part-0000.parquet"
PARTITION_KEY = "accounting_period"

# The two run identifiers every row carries, written by `write_partition` and by
# nothing else. `_first_run_id` is the run that first landed the primary key and
# survives every rewrite; `_last_run_id` belongs to whichever run wrote the file.
# Underscore-prefixed because they are ingest's rather than the source's - a general
# ledger has no column spelled this way - and outside the contract, and therefore
# outside the checksum, by construction. See docs/adr/0018.
FIRST_RUN_ID = "_first_run_id"
LAST_RUN_ID = "_last_run_id"
METADATA = (FIRST_RUN_ID, LAST_RUN_ID)

# Field separator inside a rendered row. A control character rather than a comma, and
# every field is rendered with its length in front of it. The separator alone is not
# enough: a CSV field may hold anything, this character included, and without the
# length prefix the rows ("A", "B<US>C", "D") and ("A<US>B", "C", "D") render
# identically and hash the same. The checksum is the instrument the acceptance
# criterion is read with, so it has to be injective rather than merely unlikely to
# collide.
UNIT_SEPARATOR = "\x1f"


def columns_of(contract: dict) -> list[str]:
    return [spec["name"] for spec in contract["columns"]]


def partition_of(date_text: str) -> str:
    """The accounting period a date belongs to. `2026-03-14` -> `2026-03`."""
    return date_text[:7]


def table_dir(raw_dir, table: str) -> Path:
    return Path(raw_dir) / table


def partition_path(raw_dir, contract: dict, period: str | None = None) -> Path:
    """Where one partition's file lives.

    `period` is required for a partitioned table and meaningless for one that declares
    no `partition_by`; passing it anyway is a caller confusing the two layouts, which
    is worth a message rather than a file in a directory nobody reads.
    """
    directory = table_dir(raw_dir, contract["table"])
    if "partition_by" in contract:
        if period is None:
            raise ValueError(f"{contract['table']} is partitioned, so it needs a period")
        return directory / f"{PARTITION_KEY}={period}" / PART_FILE
    if period is not None:
        raise ValueError(f"{contract['table']} is not partitioned, so {period!r} means nothing")
    return directory / PART_FILE


def partitions(contract: dict, raw_dir) -> list[Path]:
    """Every part file this table currently has, in a stable order."""
    directory = table_dir(raw_dir, contract["table"])
    if not directory.is_dir():
        return []
    if "partition_by" not in contract:
        path = directory / PART_FILE
        return [path] if path.is_file() else []
    return sorted(directory.glob(f"{PARTITION_KEY}=*/{PART_FILE}"))


def sort_key(contract: dict):
    """Order a partition by its primary key, comparing the key columns as the text
    they are. Reading `version` as a number would sort 10 before 2, which is a
    defensible reading and still a type decision this layer does not make."""
    key_columns = contract["primary_key"]
    return lambda row: tuple(row[name] for name in key_columns)


def read_keys(contract: dict, path) -> set[tuple[str, ...]]:
    """The primary keys one partition holds, without reading the rest of it.

    Parquet is columnar, so this touches the key columns and nothing else - which is
    what makes the cross-partition key lookup in `ingest.load` cheap enough to run on
    every batch. See docs/adr/0016.
    """
    path = Path(path)
    if not path.is_file():
        return set()
    key_columns = contract["primary_key"]
    table = pq.read_table(path, columns=key_columns)
    return set(zip(*(table.column(name).to_pylist() for name in key_columns)))


def read_partition(contract: dict, path, *, metadata: bool = False) -> list[dict[str, str]]:
    """One partition's rows, in file order, holding only the declared columns.

    `metadata=True` adds the two run identifiers. It is opt-in so that every caller
    reading rows for what the source said keeps seeing exactly that; the callers that
    have to ask are the ones about to write those rows back, because a rewrite that
    read without the identifiers would drop them. See docs/adr/0018.
    """
    path = Path(path)
    if not path.is_file():
        return []
    declared = columns_of(contract)
    if not metadata:
        table = pq.read_table(path, columns=declared)
        return [dict(zip(declared, values)) for values in zip(*(
            column.to_pylist() for column in table.columns
        ))]

    # A partition written before the identifiers existed does not carry them. Asking
    # pyarrow for a column that is not there raises; reading what is there and leaving
    # the rest empty says "not recorded" rather than inventing a run.
    present = [name for name in METADATA if name in pq.read_schema(path).names]
    wanted = declared + present
    table = pq.read_table(path, columns=wanted)
    rows = [dict(zip(wanted, values)) for values in zip(*(
        column.to_pylist() for column in table.columns
    ))]
    for row in rows:
        for name in METADATA:
            row.setdefault(name, "")
    return rows


def write_partition(contract: dict, path, rows, *, run_id: str) -> None:
    """Write one partition, sorted by primary key, replacing whatever was there.

    The write goes to a temporary file beside the target and is moved over it, so an
    interrupted write leaves the old partition whole rather than half of a new one.
    See docs/adr/0016-a-merge-rewrites-the-affected-partitions-whole.md.

    This is the only place the two run identifiers are stamped, so every write path
    obeys one rule: `_last_run_id` is this run, and `_first_run_id` is whatever the row
    already carried, or this run when it carried nothing. `run_id` is required rather
    than defaulted - a default would let a write path that forgot to thread it drop the
    identifiers in silence, which is the failure this ticket exists to prevent. See
    docs/adr/0018.
    """
    path = Path(path)
    declared = columns_of(contract)
    ordered = sorted(rows, key=sort_key(contract))
    columns = declared + list(METADATA)
    stamped = {
        FIRST_RUN_ID: [row.get(FIRST_RUN_ID) or run_id for row in ordered],
        LAST_RUN_ID: [run_id] * len(ordered),
    }

    # Built before anything is opened: a row missing a declared column raises here,
    # with nothing on disk touched and no temporary file to clean up.
    table = pa.table(
        {name: pa.array(stamped[name] if name in stamped
                        else [row[name] for row in ordered], pa.string())
         for name in columns},
        schema=pa.schema([(name, pa.string()) for name in columns]),
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        pq.write_table(table, temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def prior_first_run_ids(contract: dict, raw_dir) -> dict[tuple[str, ...], str]:
    """Which run first landed each key this table already holds.

    Only the key columns and one metadata column are read; Parquet is columnar, so this
    does not pay for the rest of the row. It exists for the whole-table replacement
    path, which - unlike a merge - has no other reason to open the old file, and would
    otherwise report every surviving row as first landed by the run that merely
    replaced it. See docs/adr/0018.
    """
    key_columns = contract["primary_key"]
    wanted = key_columns + [FIRST_RUN_ID]
    prior: dict[tuple[str, ...], str] = {}
    for path in partitions(contract, raw_dir):
        if FIRST_RUN_ID not in pq.read_schema(path).names:
            continue
        table = pq.read_table(path, columns=wanted)
        for values in zip(*(table.column(name).to_pylist() for name in wanted)):
            prior[tuple(values[:-1])] = values[-1]
    return prior


def write_table(contract: dict, raw_dir, rows, *, run_id: str) -> list[str]:
    """Replace a table's contents with `rows`. Returns the periods written.

    This is what a table declaring no watermark is loaded with: no merge, no delete
    semantics to invent, and a row the source no longer carries is simply gone. The old
    file is read for its `_first_run_id` values and for nothing else - carrying those
    across is not a merge, and a key the incoming rows do not carry stays gone.
    """
    directory = table_dir(raw_dir, contract["table"])
    key_columns = contract["primary_key"]
    prior = prior_first_run_ids(contract, raw_dir)
    rows = [
        {**row, FIRST_RUN_ID: prior.get(tuple(row[name] for name in key_columns)) or run_id}
        for row in rows
    ]

    if "partition_by" not in contract:
        write_partition(contract, partition_path(raw_dir, contract), rows, run_id=run_id)
        return []

    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault(partition_of(row[contract["partition_by"]]), []).append(row)

    # The new partitions go down before the stale ones come up. The other order loses
    # data on a batch that cannot be serialised: February would already be deleted by
    # the time January raised, and nothing would have replaced it.
    for period, group in sorted(grouped.items()):
        write_partition(contract, partition_path(raw_dir, contract, period), group,
                        run_id=run_id)

    for path in partitions(contract, raw_dir):
        if path.parent.name.split("=", 1)[1] not in grouped:
            path.unlink()
            path.parent.rmdir()

    directory.mkdir(parents=True, exist_ok=True)
    return sorted(grouped)


def read_table(contract: dict, raw_dir):
    """Every row of a table, partition by partition. Partitions are read whole; the
    table is not."""
    for path in partitions(contract, raw_dir):
        yield from read_partition(contract, path)


def row_count(contract: dict, raw_dir) -> int:
    return sum(pq.read_metadata(path).num_rows for path in partitions(contract, raw_dir))


def checksum(contract: dict, raw_dir) -> str:
    """A digest of the rows a table holds, over the declared columns only.

    Not the file bytes: Parquet carries the writer's version and its own encoding
    choices, so hashing them would measure the build environment. Not any ingestion
    metadata either - `record-every-pipeline-run` will add an ingestion time that
    differs on every run, and it is outside this by construction rather than by a list
    someone has to maintain. See docs/adr/0017-the-raw-checksum-is-over-rows-not-bytes.md.

    Sorting rather than trusting file order is deliberate. The partitions are already
    written sorted, and an instrument that shared that assumption could not detect the
    assumption breaking.
    """
    declared = columns_of(contract)
    rendered = sorted(
        UNIT_SEPARATOR.join(f"{len(row[name])}:{row[name]}" for name in declared)
        for row in read_table(contract, raw_dir)
    )
    digest = hashlib.sha256()
    for line in rendered:
        digest.update(line.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()
