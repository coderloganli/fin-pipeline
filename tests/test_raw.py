"""The raw layer: where a landed row lives, and what makes two runs comparable.

Raw holds what arrived. Every column is a Parquet string, an empty field stays an
empty string, and the only columns present are the ones the contract declares -
retyping is `staging`'s line in the layering table, and a raw layer that already
reinterpreted cannot answer the question it exists for. See docs/adr/0015.

The checksum is the instrument the acceptance criterion is read with: the same batch
loaded three times leaves the row count and the checksum unchanged. It is computed
over rows rather than over file bytes, because Parquet bytes carry the writer's
version and because ingestion metadata - which `record-every-pipeline-run` will add,
and which differs on every run - must not reach it. See docs/adr/0017.

Cases 9-18 of task.md.
"""

from pathlib import Path

import pytest

from ingest import contracts, raw

GL_ENTRY = contracts.load("gl_entry")
FX_RATE = contracts.load("fx_rate")
DIM_ACCOUNT = contracts.load("dim_account_src")

PART_FILE = "part-0000.parquet"


def entry(entry_id: str, version: str, accounting_date: str, **overrides) -> dict[str, str]:
    """One gl_entry row, every declared column present and every value a string."""
    row = {
        "entry_id": entry_id,
        "version": version,
        "accounting_date": accounting_date,
        "posted_at": accounting_date,
        "account_code": "6001",
        "cost_center_code": "CC01",
        "currency": "CNY",
        "amount_dr": "100.00",
        "amount_cr": "0.00",
        "doc_id": f"DOC-{entry_id}",
    }
    row.update(overrides)
    return row


THREE_PERIODS = [
    entry("E1", "1", "2026-01-15"),
    entry("E2", "1", "2026-02-15"),
    entry("E3", "1", "2026-03-15"),
    entry("E4", "1", "2026-03-20"),
]


def test_a_batch_lands_one_file_per_accounting_period(tmp_path):
    """Case 9. The partition is what lets a late correction to March rewrite March
    and open nothing else."""
    raw.write_table(GL_ENTRY, tmp_path, THREE_PERIODS)

    directories = sorted(path.name for path in (tmp_path / "gl_entry").iterdir())
    assert directories == [
        "accounting_period=2026-01",
        "accounting_period=2026-02",
        "accounting_period=2026-03",
    ]
    for directory in directories:
        period = directory.split("=")[1]
        path = tmp_path / "gl_entry" / directory / PART_FILE
        assert path.is_file(), f"{directory} holds no part file"
        for row in raw.read_partition(GL_ENTRY, path):
            assert row["accounting_date"][:7] == period


def test_a_partition_carries_the_declared_columns_as_strings(tmp_path):
    """Case 10. Nothing beside the contract's columns, in the contract's order, and
    every one of them a string."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    raw.write_table(GL_ENTRY, tmp_path, THREE_PERIODS)
    schema = pq.read_schema(tmp_path / "gl_entry" / "accounting_period=2026-01" / PART_FILE)

    assert schema.names == [spec["name"] for spec in GL_ENTRY["columns"]]
    for name in schema.names:
        assert schema.field(name).type == pa.string(), f"{name} is not a string"


def test_rows_round_trip_unchanged(tmp_path):
    """Case 11."""
    raw.write_table(GL_ENTRY, tmp_path, THREE_PERIODS)
    landed = sorted(raw.read_table(GL_ENTRY, tmp_path), key=lambda row: row["entry_id"])

    assert landed == sorted(THREE_PERIODS, key=lambda row: row["entry_id"])
    for row in landed:
        for name, value in row.items():
            assert isinstance(value, str), f"{name} came back as {type(value).__name__}"


def test_an_empty_field_stays_an_empty_string(tmp_path):
    """Case 12. `contracts.is_null` says an empty field is the null value, but that is
    what a contract *means*; applying it is a transformation, and raw does not
    transform. A top-level account has no parent."""
    rows = [
        {"account_code": "1000", "name": "Assets", "parent_code": "",
         "account_type": "asset", "effective_date": "2026-01-01"},
    ]
    raw.write_table(DIM_ACCOUNT, tmp_path, rows)

    landed = list(raw.read_table(DIM_ACCOUNT, tmp_path))
    assert landed == rows
    assert landed[0]["parent_code"] == ""


def test_a_table_with_no_partition_column_is_one_file(tmp_path):
    """Case 13. `fx_rate` declares no `partition_by`, so there is no period directory
    to put it under."""
    rows = [
        {"currency": "EUR", "rate_date": "2026-01-01", "rate_to_base": "7.654321"},
        {"currency": "USD", "rate_date": "2026-01-01", "rate_to_base": "7.123456"},
    ]
    raw.write_table(FX_RATE, tmp_path, rows)

    assert (tmp_path / "fx_rate" / PART_FILE).is_file()
    assert [path.name for path in (tmp_path / "fx_rate").iterdir()] == [PART_FILE]


def test_a_partition_is_sorted_by_primary_key_whatever_order_it_arrived_in(tmp_path):
    """Case 14. A partition's contents being a function of its rows and not of their
    arrival order is what makes two runs comparable at all."""
    forwards = [
        entry("E2", "1", "2026-01-02"),
        entry("E1", "2", "2026-01-01"),
        entry("E1", "1", "2026-01-03"),
    ]
    backwards = list(reversed(forwards))

    raw.write_table(GL_ENTRY, tmp_path / "a", forwards)
    raw.write_table(GL_ENTRY, tmp_path / "b", backwards)

    partition = f"gl_entry/accounting_period=2026-01/{PART_FILE}"
    one = raw.read_partition(GL_ENTRY, tmp_path / "a" / partition)
    other = raw.read_partition(GL_ENTRY, tmp_path / "b" / partition)

    assert one == other
    keys = [(row["entry_id"], row["version"]) for row in one]
    assert keys == sorted(keys)


def test_row_count_spans_every_partition(tmp_path):
    """Case 15."""
    raw.write_table(GL_ENTRY, tmp_path, THREE_PERIODS)
    assert raw.row_count(GL_ENTRY, tmp_path) == len(THREE_PERIODS)


def test_the_checksum_does_not_depend_on_order_or_batching(tmp_path):
    """Case 16. The same rows, written in a different order and in a different number
    of calls, must produce the same digest - otherwise the acceptance criterion is
    measuring the load's shape rather than its result."""
    raw.write_table(GL_ENTRY, tmp_path / "one_go", THREE_PERIODS)

    second = tmp_path / "in_two"
    raw.write_table(GL_ENTRY, second, list(reversed(THREE_PERIODS[:2])))
    for row in THREE_PERIODS[2:]:
        path = raw.partition_path(second, GL_ENTRY, raw.partition_of(row["accounting_date"]))
        existing = raw.read_partition(GL_ENTRY, path) if path.is_file() else []
        raw.write_partition(GL_ENTRY, path, existing + [row])

    assert raw.checksum(GL_ENTRY, tmp_path / "one_go") == raw.checksum(GL_ENTRY, second)


def test_the_checksum_notices_a_changed_field(tmp_path):
    """Case 17. An instrument that never moves is not an instrument."""
    raw.write_table(GL_ENTRY, tmp_path / "before", THREE_PERIODS)

    changed = [dict(row) for row in THREE_PERIODS]
    changed[0]["amount_dr"] = "100.01"
    raw.write_table(GL_ENTRY, tmp_path / "after", changed)

    assert raw.checksum(GL_ENTRY, tmp_path / "before") != raw.checksum(
        GL_ENTRY, tmp_path / "after"
    )


def test_the_checksum_ignores_a_column_the_contract_does_not_declare(tmp_path):
    """Case 18. `record-every-pipeline-run` will stamp a run identifier and an
    ingestion time onto these files. The ingestion time differs on every run, so a
    checksum that saw it would make this ticket's property unverifiable the moment
    that ticket lands. Simulated here by writing the extra column directly."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    raw.write_table(GL_ENTRY, tmp_path, THREE_PERIODS)
    before = raw.checksum(GL_ENTRY, tmp_path)

    path = tmp_path / "gl_entry" / "accounting_period=2026-01" / PART_FILE
    table = pq.read_table(path)
    pq.write_table(
        table.append_column(
            "ingested_at",
            pa.array(["2026-08-31T01:02:03"] * table.num_rows, pa.string()),
        ),
        path,
    )

    assert raw.checksum(GL_ENTRY, tmp_path) == before


# --- Added at stage 8 -------------------------------------------------------

def test_the_checksum_separates_fields_it_cannot_be_fooled_across(tmp_path):
    """Joining fields on a separator is not enough when a field may contain that
    separator. These two rows differ only in which side of the boundary the separator
    sits on, so a plain join renders both to the same string and hashes them the same.
    Every field carries its length, which makes the rendering injective.

    A CSV field really can hold a control character: no contract rule forbids one, so
    `ingest.validate` passes it through without a word."""
    separator = ""
    left = [entry("A", "1", "2026-01-01",
                  account_code=f"6001{separator}X", cost_center_code="CC01")]
    right = [entry("A", "1", "2026-01-01",
                   account_code="6001", cost_center_code=f"X{separator}CC01")]

    raw.write_table(GL_ENTRY, tmp_path / "left", left)
    raw.write_table(GL_ENTRY, tmp_path / "right", right)

    assert raw.checksum(GL_ENTRY, tmp_path / "left") != raw.checksum(
        GL_ENTRY, tmp_path / "right"
    )


def test_an_absent_raw_layer_reads_as_empty_rather_than_raising(tmp_path):
    """Every read path is asked before the first load has run - the acceptance check
    itself asks for a checksum of a table that may not be there yet."""
    missing = tmp_path / "never-written"

    assert raw.partitions(GL_ENTRY, missing) == []
    assert list(raw.read_table(GL_ENTRY, missing)) == []
    assert raw.row_count(GL_ENTRY, missing) == 0
    assert raw.checksum(GL_ENTRY, missing) == raw.checksum(GL_ENTRY, tmp_path / "also-missing")
    assert raw.read_partition(GL_ENTRY, missing / "gl_entry" / PART_FILE) == []


@pytest.mark.parametrize("contract,period,expected", [
    (GL_ENTRY, None, "needs a period"),
    (FX_RATE, "2026-01", "not partitioned"),
])
def test_asking_for_the_wrong_kind_of_partition_is_refused(tmp_path, contract, period, expected):
    """The two layouts are not interchangeable, and confusing them would otherwise
    write a file into a directory nothing ever reads."""
    with pytest.raises(ValueError, match=expected):
        raw.partition_path(tmp_path, contract, period)


def test_read_keys_on_an_absent_partition_is_empty(tmp_path):
    """The cross-partition key sweep asks every partition, including ones a concurrent
    delete has taken away underneath it."""
    assert raw.read_keys(GL_ENTRY, tmp_path / "gone" / PART_FILE) == set()


def test_read_keys_reads_the_keys_and_not_the_rest(tmp_path):
    raw.write_table(GL_ENTRY, tmp_path, THREE_PERIODS)
    path = tmp_path / "gl_entry" / "accounting_period=2026-03" / PART_FILE

    assert raw.read_keys(GL_ENTRY, path) == {("E3", "1"), ("E4", "1")}


def test_a_parquet_write_that_fails_leaves_no_temporary_file(tmp_path, monkeypatch):
    """The temporary file is cleaned up whether the write reached the move or not.
    Debris beside a partition would be picked up by nothing and explained by nobody."""
    import pyarrow.parquet as pq

    raw.write_table(GL_ENTRY, tmp_path, THREE_PERIODS)
    path = tmp_path / "gl_entry" / "accounting_period=2026-01" / PART_FILE
    before = path.read_bytes()

    def fails_after_opening(table, where, **kwargs):
        Path(where).write_bytes(b"half a file")
        raise OSError("the disk filled up")

    monkeypatch.setattr(pq, "write_table", fails_after_opening)
    with pytest.raises(OSError):
        raw.write_partition(GL_ENTRY, path, [entry("E9", "1", "2026-01-05")])
    monkeypatch.undo()

    assert path.read_bytes() == before
    assert [p.name for p in path.parent.iterdir()] == [PART_FILE]


def test_a_partitioned_table_reloaded_with_nothing_keeps_its_directory(tmp_path):
    """The table still exists, it just holds no periods. Removing the directory as well
    would make `partitions` and `checksum` unable to tell a table that was emptied from
    one that was never loaded - and the acceptance check reads both."""
    raw.write_table(GL_ENTRY, tmp_path, THREE_PERIODS)
    raw.write_table(GL_ENTRY, tmp_path, [])

    assert (tmp_path / "gl_entry").is_dir()
    assert list((tmp_path / "gl_entry").iterdir()) == []
    assert raw.partitions(GL_ENTRY, tmp_path) == []
    assert raw.row_count(GL_ENTRY, tmp_path) == 0
