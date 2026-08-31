"""The raw layer: where a landed row lives, and what makes two runs comparable.

Raw holds the source as it arrived. Every column is written as a Parquet string, an
empty field stays an empty string, and nothing is written beside the columns the
contract declares. Unifying types is `staging`'s line in the layering table, and a raw
layer that already reinterpreted cannot answer the question it exists for - whether
the source really said that. See docs/adr/0015-raw-lands-every-column-as-text.md.

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


def read_partition(contract: dict, path) -> list[dict[str, str]]:
    """One partition's rows, in file order, holding only the declared columns."""
    path = Path(path)
    if not path.is_file():
        return []
    declared = columns_of(contract)
    table = pq.read_table(path, columns=declared)
    return [dict(zip(declared, values)) for values in zip(*(
        column.to_pylist() for column in table.columns
    ))]


def write_partition(contract: dict, path, rows) -> None:
    """Write one partition, sorted by primary key, replacing whatever was there.

    The write goes to a temporary file beside the target and is moved over it, so an
    interrupted write leaves the old partition whole rather than half of a new one.
    See docs/adr/0016-a-merge-rewrites-the-affected-partitions-whole.md.
    """
    path = Path(path)
    declared = columns_of(contract)
    ordered = sorted(rows, key=sort_key(contract))

    # Built before anything is opened: a row missing a declared column raises here,
    # with nothing on disk touched and no temporary file to clean up.
    table = pa.table(
        {name: pa.array([row[name] for row in ordered], pa.string()) for name in declared},
        schema=pa.schema([(name, pa.string()) for name in declared]),
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        pq.write_table(table, temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_table(contract: dict, raw_dir, rows) -> list[str]:
    """Replace a table's contents with `rows`. Returns the periods written.

    This is what a table declaring no watermark is loaded with: no merge, no delete
    semantics to invent, and a row the source no longer carries is simply gone.
    """
    directory = table_dir(raw_dir, contract["table"])
    if "partition_by" not in contract:
        write_partition(contract, partition_path(raw_dir, contract), list(rows))
        return []

    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault(partition_of(row[contract["partition_by"]]), []).append(row)

    # The new partitions go down before the stale ones come up. The other order loses
    # data on a batch that cannot be serialised: February would already be deleted by
    # the time January raised, and nothing would have replaced it.
    for period, group in sorted(grouped.items()):
        write_partition(contract, partition_path(raw_dir, contract, period), group)

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
