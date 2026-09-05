"""The watermarked incremental load and the merge that makes a rerun free.

The property this whole module exists to establish: the same batch loaded three times
leaves the raw layer's row count and checksum unchanged. Everything else here is
either what that property needs in order to be true, or the cost of getting it - the
overlap window catches an update that arrives late, and misses one that arrives later
still, and both halves are asserted rather than left in a comment.

See docs/adr/0014 (the contract names the watermark column), 0016 (a merge rewrites
the affected partitions whole and the watermark moves last).

Cases 19-40 of task.md.
"""

import csv
import json

from pathlib import Path

import pytest

from ingest import contracts, load, raw, runs

GL_ENTRY = contracts.load("gl_entry")
GL_ADJUSTMENT = contracts.load("gl_adjustment")
FX_RATE = contracts.load("fx_rate")

PART_FILE = "part-0000.parquet"

# Two runs, for the tests that need to tell them apart. Every write needs one:
# `write_partition` stamps the row with it. See docs/adr/0018.

# --- building a source directory -------------------------------------------

def entry(entry_id, version, accounting_date, posted_at, **overrides):
    row = {
        "entry_id": entry_id,
        "version": version,
        "accounting_date": accounting_date,
        "posted_at": posted_at,
        "account_code": "6001",
        "cost_center_code": "CC01",
        "currency": "CNY",
        "amount_dr": "100.00",
        "amount_cr": "0.00",
        "doc_id": f"DOC-{entry_id}",
        "vendor_code": "V-0001",
        "description": "Office supplies purchase",
    }
    row.update(overrides)
    return row


def adjustment(entry_id, version, accounting_date, posted_at, **overrides):
    row = entry(entry_id, version, accounting_date, posted_at)
    row["adjusts_entry_id"] = "E1"
    row["adjustment_type"] = "correction"
    row.update(overrides)
    return row


# Four rows over three accounting periods. The newest arrival is 2026-03-21, so with
# the default seven-day window a second run reaches back to 2026-03-14 and re-reads
# exactly the two March rows.
SOURCE_ROWS = [
    entry("E1", "1", "2026-01-15", "2026-01-20"),
    entry("E2", "1", "2026-02-15", "2026-02-20"),
    entry("E3", "1", "2026-03-15", "2026-03-20"),
    entry("E4", "1", "2026-03-16", "2026-03-21"),
]

HIGHEST_ARRIVAL = "2026-03-21"
INSIDE_THE_WINDOW = "2026-03-18"
BEFORE_THE_WINDOW = "2026-03-01"


def write_source(source_dir, contract, rows):
    """Write one source table as the generator would: the contract's columns, in the
    contract's order, one row at a time."""
    source_dir.mkdir(parents=True, exist_ok=True)
    columns = [spec["name"] for spec in contract["columns"]]
    path = source_dir / f"{contract['table']}.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            writer.writerow([row[column] for column in columns])
    return path


@pytest.fixture
def source(tmp_path):
    write_source(tmp_path / "source", GL_ENTRY, SOURCE_ROWS)
    return tmp_path / "source"


@pytest.fixture
def raw_dir(tmp_path):
    return tmp_path / "raw"


def run(source, raw_dir, **kwargs):
    """One load of gl_entry, the way the CLI would drive it."""
    kwargs.setdefault("tables", ["gl_entry"])
    return load.load_source(source, raw_dir, **kwargs)


def stored_watermarks(raw_dir):
    path = raw_dir / "_state" / "watermarks.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def landed(raw_dir, contract=GL_ENTRY):
    return {(row["entry_id"], row["version"]): row
            for row in raw.read_table(contract, raw_dir)}


# --- the merge -------------------------------------------------------------

def test_an_empty_raw_layer_takes_every_source_row(source, raw_dir):
    """Case 19."""
    run(source, raw_dir)
    assert raw.row_count(GL_ENTRY, raw_dir) == len(SOURCE_ROWS)


def test_three_runs_of_the_same_batch_leave_the_raw_layer_identical(source, raw_dir):
    """Case 20. The acceptance criterion, in the words the ticket was written in."""
    seen = []
    for _ in range(3):
        run(source, raw_dir)
        seen.append((raw.row_count(GL_ENTRY, raw_dir), raw.checksum(GL_ENTRY, raw_dir)))

    assert seen[0] == seen[1] == seen[2], seen
    assert seen[0][0] == len(SOURCE_ROWS)


def test_the_same_key_arriving_changed_replaces_the_row(source, raw_dir):
    """Case 21. A source that edits a row in place without bumping its version is not
    two rows in raw, and the newer value is the one that stands."""
    run(source, raw_dir)

    changed = [dict(row) for row in SOURCE_ROWS]
    changed[2]["amount_dr"] = "999.00"
    write_source(source, GL_ENTRY, changed)
    run(source, raw_dir)

    rows = landed(raw_dir)
    assert len(rows) == len(SOURCE_ROWS)
    assert rows[("E3", "1")]["amount_dr"] == "999.00"


def test_a_row_that_moves_period_does_not_leave_its_old_self_behind(source, raw_dir):
    """Case 21, at the boundary the merge alone does not cover. A partition comes from
    the accounting date, which is not part of the primary key: re-dating an entry into
    another period without bumping its version lands it in a new partition while the old
    one still holds the same key. Merging the new period alone leaves two rows for one
    key, which is exactly what `(entry_id, version)` is there to prevent."""
    run(source, raw_dir)
    assert ("E3", "1") in landed(raw_dir)

    moved = [dict(row) for row in SOURCE_ROWS]
    moved[2]["accounting_date"] = "2026-04-01"
    moved[2]["posted_at"] = "2026-04-02"
    write_source(source, GL_ENTRY, moved)
    report = run(source, raw_dir)

    rows = list(raw.read_table(GL_ENTRY, raw_dir))
    keys = [(row["entry_id"], row["version"]) for row in rows]
    assert len(keys) == len(set(keys)), f"raw carries a repeated primary key: {keys}"
    assert len(rows) == len(SOURCE_ROWS)

    landed_rows = landed(raw_dir)
    assert landed_rows[("E3", "1")]["accounting_date"] == "2026-04-01"
    assert report.tables[0].rows_evicted == 1


def test_a_period_that_empties_out_leaves_no_directory_behind(source, raw_dir):
    """February holds one row. Move it, and the period is gone rather than left as a
    file holding nothing - a layout that claimed the period existed would make the
    partition list disagree with the rows."""
    run(source, raw_dir)
    assert (raw_dir / "gl_entry" / "accounting_period=2026-02").is_dir()

    moved = [dict(row) for row in SOURCE_ROWS]
    moved[1]["accounting_date"] = "2026-04-01"
    moved[1]["posted_at"] = "2026-04-02"
    write_source(source, GL_ENTRY, moved)
    run(source, raw_dir)

    assert not (raw_dir / "gl_entry" / "accounting_period=2026-02").exists()
    assert raw.row_count(GL_ENTRY, raw_dir) == len(SOURCE_ROWS)


def test_a_run_that_moves_a_row_is_still_idempotent(source, raw_dir):
    """Eviction is a delete, and a delete is where an idempotent load usually stops
    being one."""
    run(source, raw_dir)

    moved = [dict(row) for row in SOURCE_ROWS]
    moved[2]["accounting_date"] = "2026-04-01"
    moved[2]["posted_at"] = "2026-04-02"
    write_source(source, GL_ENTRY, moved)

    seen = []
    for _ in range(3):
        run(source, raw_dir)
        seen.append((raw.row_count(GL_ENTRY, raw_dir), raw.checksum(GL_ENTRY, raw_dir)))

    assert seen[0] == seen[1] == seen[2], seen


def test_a_correction_arrives_as_a_new_version_and_both_rows_stay(source, raw_dir):
    """Case 22. The key is the pair, so a correction adds a row rather than losing the
    one it corrects - which is what the as-reported view downstream needs."""
    run(source, raw_dir)

    correction = entry("E3", "2", "2026-03-15", "2026-03-22", amount_dr="120.00")
    write_source(source, GL_ENTRY, SOURCE_ROWS + [correction])
    run(source, raw_dir)

    rows = landed(raw_dir)
    assert ("E3", "1") in rows and ("E3", "2") in rows
    assert rows[("E3", "1")]["amount_dr"] == "100.00"
    assert rows[("E3", "2")]["amount_dr"] == "120.00"


def test_a_key_repeated_inside_one_batch_collapses_to_the_last(source, raw_dir):
    """Case 23. `ingest.validate` is what reports that the source repeated a key; the
    merge's job is not to turn it into two rows."""
    repeated = SOURCE_ROWS + [entry("E3", "1", "2026-03-15", "2026-03-20", amount_dr="7.00")]
    write_source(source, GL_ENTRY, repeated)
    run(source, raw_dir)

    rows = landed(raw_dir)
    assert len(rows) == len(SOURCE_ROWS)
    assert rows[("E3", "1")]["amount_dr"] == "7.00"


def test_a_batch_rewrites_only_the_periods_it_touches(source, raw_dir):
    """Case 24. Not opening the other eleven months is the point of partitioning by
    accounting period at all."""
    run(source, raw_dir)
    before = {
        path: path.read_bytes()
        for path in (raw_dir / "gl_entry").glob(f"accounting_period=*/{PART_FILE}")
        if path.parent.name != "accounting_period=2026-03"
    }
    assert len(before) == 2, "January and February are the periods that must not move"

    march_only = [entry("E5", "1", "2026-03-25", "2026-03-26")]
    write_source(source, GL_ENTRY, SOURCE_ROWS + march_only)
    run(source, raw_dir)

    for path, bytes_before in before.items():
        assert path.read_bytes() == bytes_before, f"{path.parent.name} was rewritten"


def test_a_merge_keeps_the_rows_it_did_not_touch(source, raw_dir):
    """Case 25."""
    run(source, raw_dir)

    write_source(source, GL_ENTRY, SOURCE_ROWS + [entry("E5", "1", "2026-03-25", "2026-03-26")])
    run(source, raw_dir)

    rows = landed(raw_dir)
    assert ("E4", "1") in rows
    assert rows[("E4", "1")] == SOURCE_ROWS[3]


def test_merge_partition_reports_what_it_inserted_and_updated(raw_dir):
    """Case 26."""
    path = raw.partition_path(raw_dir, GL_ENTRY, "2026-03")
    first = [entry("E3", "1", "2026-03-15", "2026-03-20")]
    assert load.merge_partition(GL_ENTRY, path, first, run_id=RUN_A) == (1, 0)

    again = [
        entry("E3", "1", "2026-03-15", "2026-03-20", amount_dr="5.00"),
        entry("E9", "1", "2026-03-18", "2026-03-19"),
    ]
    assert load.merge_partition(GL_ENTRY, path, again, run_id=RUN_B) == (1, 1)


def test_merge_partition_counts_keys_rather_than_rows(raw_dir):
    """Two incoming rows for one key produce one row, so reporting two insertions would
    make the summary line disagree with the partition it describes. `load_table`
    collapses a repeated key before it gets here; this does not rely on that."""
    path = raw.partition_path(raw_dir, GL_ENTRY, "2026-03")
    twice = [
        entry("E3", "1", "2026-03-15", "2026-03-20", amount_dr="1.00"),
        entry("E3", "1", "2026-03-15", "2026-03-20", amount_dr="2.00"),
    ]

    assert load.merge_partition(GL_ENTRY, path, twice, run_id=RUN_A) == (1, 0)
    assert raw.read_partition(GL_ENTRY, path) == [twice[1]]


@pytest.mark.parametrize("value", ["2026-2-01", "2026-03-1", "2026-13-01", "", 42, None])
def test_a_non_canonical_stored_watermark_is_refused(source, raw_dir, value):
    """`2026-2-01` parses as a date and then sorts after `2026-10-01`, so the watermark
    would silently stop advancing - a run reporting success while reading less and less
    of the source. The comparison is on text, so the text has to be canonical."""
    run(source, raw_dir)
    (raw_dir / "_state" / "watermarks.json").write_text(
        json.dumps({"gl_entry": value}), encoding="utf-8"
    )

    with pytest.raises(load.StateError, match="unusable|not a date"):
        run(source, raw_dir)


def test_the_adjustment_table_is_loaded_incrementally_too(tmp_path, raw_dir):
    """Case 26a. `gl_adjustment` is the second table that declares a watermark, and it
    could be wired wrong while every gl_entry assertion stayed green."""
    source = tmp_path / "source"
    rows = [
        adjustment("A1", "1", "2026-01-15", "2026-01-25"),
        adjustment("A2", "1", "2026-03-15", "2026-03-25"),
    ]
    write_source(source, GL_ENTRY, SOURCE_ROWS)
    write_source(source, GL_ADJUSTMENT, rows)

    seen = []
    for _ in range(3):
        load.load_source(source, raw_dir, tables=["gl_entry", "gl_adjustment"])
        seen.append((raw.row_count(GL_ADJUSTMENT, raw_dir),
                     raw.checksum(GL_ADJUSTMENT, raw_dir)))

    assert seen[0] == seen[1] == seen[2], seen
    assert raw.row_count(GL_ADJUSTMENT, raw_dir) == len(rows)
    assert raw.row_count(GL_ENTRY, raw_dir) == len(SOURCE_ROWS)
    assert (raw_dir / "gl_adjustment").is_dir() and (raw_dir / "gl_entry").is_dir()


def test_a_failed_replacement_leaves_the_existing_partition_intact(raw_dir, monkeypatch):
    """Case 26b. The write goes to a temporary file and is moved over the old one, so a
    failure during the move leaves the old partition whole and leaves no debris.

    The failure is injected into the move itself rather than earlier: a write that dies
    before it opens anything proves nothing about the replacement, which is the step
    the case exists to cover."""
    path = raw.partition_path(raw_dir, GL_ENTRY, "2026-03")
    raw.write_partition(GL_ENTRY, path, [entry("E3", "1", "2026-03-15", "2026-03-20")], run_id=RUN_A)
    before = path.read_bytes()

    def refuses_to_move(source, destination):
        raise OSError("the move failed")

    monkeypatch.setattr(raw.os, "replace", refuses_to_move)
    with pytest.raises(OSError):
        raw.write_partition(GL_ENTRY, path, [entry("E9", "1", "2026-03-18", "2026-03-19")], run_id=RUN_A)
    monkeypatch.undo()

    assert path.read_bytes() == before
    assert [p.name for p in path.parent.iterdir()] == [PART_FILE], "a temporary file was left"


def test_a_row_missing_a_declared_column_never_reaches_the_disk(raw_dir):
    """The other half of case 26b: a batch that cannot be serialised must fail before
    the existing partition is opened, not halfway through replacing it."""
    path = raw.partition_path(raw_dir, GL_ENTRY, "2026-03")
    raw.write_partition(GL_ENTRY, path, [entry("E3", "1", "2026-03-15", "2026-03-20")], run_id=RUN_A)
    before = path.read_bytes()

    incomplete = dict(entry("E9", "1", "2026-03-18", "2026-03-19"))
    del incomplete["doc_id"]
    with pytest.raises(KeyError):
        raw.write_partition(GL_ENTRY, path, [incomplete], run_id=RUN_A)

    assert path.read_bytes() == before
    assert [p.name for p in path.parent.iterdir()] == [PART_FILE]


def test_the_load_report_says_exactly_what_happened(source, raw_dir):
    """Case 26c. The CLI's summary line is rendered from these; asserting the line
    alone would let a wrong number reach it and read as prose."""
    first = run(source, raw_dir).tables[0]
    assert (first.table, first.rows_scanned, first.rows_inserted, first.rows_updated,
            first.rows_evicted, first.partitions_written,
            first.watermark_from, first.watermark_to) == (
        "gl_entry", 4, 4, 0, 0, 3, None, HIGHEST_ARRIVAL)

    changed = [dict(row) for row in SOURCE_ROWS]
    changed[2]["amount_dr"] = "999.00"
    write_source(source, GL_ENTRY, changed + [entry("E5", "1", "2026-03-25", "2026-03-22")])
    second = run(source, raw_dir).tables[0]

    assert (second.rows_scanned, second.rows_inserted, second.rows_updated,
            second.rows_evicted, second.partitions_written,
            second.watermark_from, second.watermark_to) == (
        3, 1, 2, 0, 1, HIGHEST_ARRIVAL, "2026-03-22")


# --- the watermark ---------------------------------------------------------

def test_the_first_run_reads_everything_and_records_where_it_got_to(source, raw_dir):
    """Case 27."""
    report = run(source, raw_dir)

    assert report.tables[0].rows_scanned == len(SOURCE_ROWS)
    assert stored_watermarks(raw_dir)["gl_entry"] == HIGHEST_ARRIVAL


def test_the_second_run_reads_only_the_window(source, raw_dir):
    """Case 28. Reading less is the only reason any of this exists."""
    run(source, raw_dir)
    second = run(source, raw_dir).tables[0]

    assert second.rows_scanned < len(SOURCE_ROWS)
    assert second.rows_scanned == 2, "the two March rows are the ones inside the window"


def test_an_update_that_arrives_inside_the_window_is_picked_up(source, raw_dir):
    """Case 29. This is what the overlap is for: `posted_at` is a date, so an update
    that lands later on a day already passed is invisible to the watermark alone."""
    run(source, raw_dir)

    late = entry("E6", "1", "2026-03-10", INSIDE_THE_WINDOW)
    write_source(source, GL_ENTRY, SOURCE_ROWS + [late])
    run(source, raw_dir)

    assert ("E6", "1") in landed(raw_dir)


def test_an_update_that_arrives_before_the_window_is_missed(source, raw_dir):
    """Case 30. The cost of a watermarked load, asserted rather than left in a
    comment. `--full` is what recovers from it - case 33."""
    run(source, raw_dir)

    too_late = entry("E7", "1", "2026-02-10", BEFORE_THE_WINDOW)
    write_source(source, GL_ENTRY, SOURCE_ROWS + [too_late])
    run(source, raw_dir)

    assert ("E7", "1") not in landed(raw_dir)


def test_the_watermark_never_moves_backwards(source, raw_dir):
    """Case 31. A watermark that followed the batch rather than the maximum would step
    back whenever a run happened to see only older rows, and the next run would then
    re-read work it had already done - or, with a narrower window, skip it."""
    run(source, raw_dir)

    older = [entry("E8", "1", "2026-03-14", INSIDE_THE_WINDOW)]
    write_source(source, GL_ENTRY, older)
    run(source, raw_dir)

    assert stored_watermarks(raw_dir)["gl_entry"] == HIGHEST_ARRIVAL


def test_a_run_that_selects_nothing_changes_nothing(source, raw_dir):
    """Case 32."""
    run(source, raw_dir)
    before = (raw.row_count(GL_ENTRY, raw_dir), raw.checksum(GL_ENTRY, raw_dir),
              stored_watermarks(raw_dir))

    write_source(source, GL_ENTRY, [entry("E9", "1", "2026-01-05", "2026-01-06")])
    report = run(source, raw_dir)

    assert report.tables[0].rows_scanned == 0
    assert (raw.row_count(GL_ENTRY, raw_dir), raw.checksum(GL_ENTRY, raw_dir),
            stored_watermarks(raw_dir)) == before


def test_a_full_run_reaches_the_same_raw_layer_as_the_incremental_path(tmp_path):
    """Case 33. The backfill goes through the same merge, so it has to land the same
    answer - otherwise recovering from a missed update would introduce its own drift."""
    source = tmp_path / "source"
    write_source(source, GL_ENTRY, SOURCE_ROWS)

    incremental = tmp_path / "incremental"
    run(source, incremental)
    narrowed = run(source, incremental)
    assert narrowed.tables[0].rows_scanned < len(SOURCE_ROWS), "the window did not narrow"

    # Against the same raw layer, which already carries a watermark: a --full that
    # quietly used the watermark would scan the same two rows the run above did.
    backfill = run(source, incremental, full=True)
    assert backfill.tables[0].rows_scanned == len(SOURCE_ROWS)
    assert backfill.tables[0].watermark_from == HIGHEST_ARRIVAL

    fresh = tmp_path / "fresh"
    run(source, fresh, full=True)
    assert raw.row_count(GL_ENTRY, incremental) == raw.row_count(GL_ENTRY, fresh)
    assert raw.checksum(GL_ENTRY, incremental) == raw.checksum(GL_ENTRY, fresh)


def test_an_interrupted_run_leaves_the_watermark_alone_and_the_next_one_converges(
    source, raw_dir, monkeypatch
):
    """Case 34. The whole ordering argument in one test: a watermark advanced before
    the write would turn an interrupted run into permanently missing rows, and that is
    the failure that does not announce itself."""
    written = []
    real = raw.write_partition

    def fails_on_the_second(contract, path, rows, *, run_id):
        written.append(path)
        if len(written) == 2:
            raise OSError("disk went away")
        return real(contract, path, rows, run_id=run_id)

    monkeypatch.setattr(raw, "write_partition", fails_on_the_second)
    with pytest.raises(OSError):
        run(source, raw_dir)
    monkeypatch.undo()

    assert stored_watermarks(raw_dir).get("gl_entry") is None

    run(source, raw_dir)

    clean = raw_dir.parent / "clean"
    run(source, clean)
    assert raw.row_count(GL_ENTRY, raw_dir) == raw.row_count(GL_ENTRY, clean)
    assert raw.checksum(GL_ENTRY, raw_dir) == raw.checksum(GL_ENTRY, clean)


def test_an_interrupted_incremental_run_leaves_the_stored_watermark_where_it_was(
    source, raw_dir, monkeypatch
):
    """Case 34 with a watermark already on disk - the case that has something to lose.
    The first-load version above only proves no watermark appears; this proves an
    existing one is neither moved nor removed by a run that died halfway."""
    run(source, raw_dir)
    assert stored_watermarks(raw_dir)["gl_entry"] == HIGHEST_ARRIVAL

    arriving = SOURCE_ROWS + [
        entry("E5", "1", "2026-04-01", "2026-04-02"),
        entry("E6", "1", "2026-05-01", "2026-05-02"),
    ]
    write_source(source, GL_ENTRY, arriving)

    written = []
    real = raw.write_partition

    def fails_on_the_second(contract, path, rows, *, run_id):
        written.append(path)
        if len(written) == 2:
            raise OSError("disk went away")
        return real(contract, path, rows, run_id=run_id)

    monkeypatch.setattr(raw, "write_partition", fails_on_the_second)
    with pytest.raises(OSError):
        run(source, raw_dir)
    monkeypatch.undo()

    assert stored_watermarks(raw_dir)["gl_entry"] == HIGHEST_ARRIVAL

    run(source, raw_dir)
    clean = raw_dir.parent / "clean"
    run(source, clean, full=True)
    assert raw.checksum(GL_ENTRY, raw_dir) == raw.checksum(GL_ENTRY, clean)


def test_a_negative_overlap_window_is_a_usage_error(tmp_path, raw_dir):
    """A negative window moves the lower bound past the watermark, so the run skips the
    rows nearest it - the exact ones the window exists to re-read. Refused at the
    command rather than quietly narrowing the load to nothing."""
    source = tmp_path / "source"
    write_source(source, GL_ENTRY, SOURCE_ROWS)

    assert load.main(["--source", str(source), "--raw", str(raw_dir),
                      "--overlap-days", "-1"]) == 2
    with pytest.raises(ValueError, match="cannot be negative"):
        load.lower_bound(HIGHEST_ARRIVAL, -1)


def test_eviction_counts_the_rows_it_removed(source, raw_dir):
    """A partition already holding two rows for one key is exactly the state eviction
    exists to repair, so reporting "1" would understate what it did."""
    run(source, raw_dir)

    duplicated = raw.read_partition(
        GL_ENTRY, raw.partition_path(raw_dir, GL_ENTRY, "2026-03")
    )
    twice = [row for row in duplicated if row["entry_id"] == "E3"] * 2
    others = [row for row in duplicated if row["entry_id"] != "E3"]
    raw.write_partition(
        GL_ENTRY, raw.partition_path(raw_dir, GL_ENTRY, "2026-03"), others + twice,
        run_id=RUN_A,
    )

    # Only the moved row is in this batch, so March is never merged - and a merge
    # would have collapsed the duplicate on its way past. The stale copies survive to
    # the sweep, which is the state being counted.
    moved = dict(SOURCE_ROWS[2])
    moved["accounting_date"] = "2026-04-01"
    moved["posted_at"] = "2026-04-02"
    write_source(source, GL_ENTRY, [moved])

    report = run(source, raw_dir)

    assert report.tables[0].rows_evicted == 2
    keys = [(row["entry_id"], row["version"]) for row in raw.read_table(GL_ENTRY, raw_dir)]
    assert len(keys) == len(set(keys)), f"raw carries a repeated primary key: {keys}"


def test_the_watermark_temporary_file_is_cleaned_up_when_the_write_fails(
    source, raw_dir, monkeypatch
):
    """The cleanup runs whether the failure came before the move or during it."""
    run(source, raw_dir)
    state = raw_dir / "_state" / "watermarks.json"
    before = state.read_text(encoding="utf-8")

    def refuses_to_write(self, *args, **kwargs):
        if self.name.startswith("."):
            self.write_bytes(b"half")
            raise OSError("the disk filled up")
        raise AssertionError("only the temporary file should have been written")

    monkeypatch.setattr(Path, "write_text", refuses_to_write)
    with pytest.raises(OSError):
        load.Watermarks.load(raw_dir).save()
    monkeypatch.undo()

    assert state.read_text(encoding="utf-8") == before
    assert sorted(path.name for path in (raw_dir / "_state").iterdir()) == [
        "runs.jsonl", "watermarks.json"
    ]


# --- tables that declare no watermark --------------------------------------

FX_ROWS = [
    {"currency": "EUR", "rate_date": "2026-01-01", "rate_to_base": "7.654321"},
    {"currency": "USD", "rate_date": "2026-01-01", "rate_to_base": "7.123456"},
]


FULL_RELOAD = ("dim_account_src", "dim_cost_center_src", "dim_vendor", "fx_rate")


def test_a_table_that_declares_no_watermark_is_loaded_in_full(tmp_path, raw_dir):
    """Case 2. Absence is what says "load this one in full". Asserted here rather than
    among the contract tests because absence means nothing until something reads it:
    a loader that had never heard of the key would satisfy a bare `not in` too."""
    for table in FULL_RELOAD:
        contract = contracts.load(table)
        assert "watermark" not in contract, table
        assert "partition_by" not in contract, table

    source = tmp_path / "source"
    write_source(source, FX_RATE, FX_ROWS)
    load.load_source(source, raw_dir, tables=["fx_rate"])

    assert "fx_rate" not in stored_watermarks(raw_dir)
    assert [path.name for path in (raw_dir / "fx_rate").iterdir()] == [PART_FILE]


def test_a_full_reload_table_drops_a_row_the_source_no_longer_has(tmp_path, raw_dir):
    """Case 35. This is what separates a full reload from a merge: the merge has no
    delete, and a table loaded in full does not need one."""
    source = tmp_path / "source"
    write_source(source, FX_RATE, FX_ROWS)
    load.load_source(source, raw_dir, tables=["fx_rate"])

    write_source(source, FX_RATE, FX_ROWS[:1])
    load.load_source(source, raw_dir, tables=["fx_rate"])

    landed_rows = list(raw.read_table(FX_RATE, raw_dir))
    assert landed_rows == FX_ROWS[:1]


def test_a_full_reload_table_is_stable_across_runs(tmp_path, raw_dir):
    """Case 36. Loading in full is not an excuse to stop being idempotent."""
    source = tmp_path / "source"
    write_source(source, FX_RATE, FX_ROWS)

    seen = []
    for _ in range(2):
        load.load_source(source, raw_dir, tables=["fx_rate"])
        seen.append((raw.row_count(FX_RATE, raw_dir), raw.checksum(FX_RATE, raw_dir)))

    assert seen[0] == seen[1]


# --- the command -----------------------------------------------------------

def test_the_command_prints_one_line_per_table(tmp_path, raw_dir, capsys):
    """Case 37. The line is pinned rather than sampled: a summary nobody can parse is
    a summary that stops being read."""
    source = tmp_path / "source"
    write_source(source, GL_ENTRY, SOURCE_ROWS)
    write_source(source, FX_RATE, FX_ROWS)

    code = load.main(["--source", str(source), "--raw", str(raw_dir),
                      "--table", "gl_entry", "--table", "fx_rate"])

    assert code == 0
    printed = capsys.readouterr().out.splitlines()
    assert printed[0].startswith("run ")
    assert printed[1:] == [
        "gl_entry: scanned 4, inserted 4, updated 0, evicted 0, partitions 3, "
        f"watermark none -> {HIGHEST_ARRIVAL}",
        "fx_rate: scanned 2, inserted 2, updated 0, evicted 0, partitions 1, "
        "watermark none -> none",
    ]


def test_a_source_that_is_not_a_directory_is_a_usage_error(tmp_path, raw_dir):
    """Case 38. The same code `ingest.validate` returns for the same mistake."""
    assert load.main(["--source", str(tmp_path / "nope"), "--raw", str(raw_dir)]) == 2


def test_the_table_flag_scopes_the_run(tmp_path, raw_dir):
    """Case 39."""
    source = tmp_path / "source"
    write_source(source, GL_ENTRY, SOURCE_ROWS)
    write_source(source, FX_RATE, FX_ROWS)

    assert load.main(["--source", str(source), "--raw", str(raw_dir),
                      "--table", "gl_entry"]) == 0

    assert sorted(path.name for path in raw_dir.iterdir()) == ["_state", "gl_entry"]


def test_the_overlap_window_is_honoured(source, raw_dir):
    """Case 40. The right number is a property of the source system, so it has to be
    reachable from outside."""
    run(source, raw_dir)

    wide = run(source, raw_dir, overlap_days=60).tables[0].rows_scanned
    narrow = run(source, raw_dir, overlap_days=0).tables[0].rows_scanned

    assert wide == len(SOURCE_ROWS)
    assert narrow < wide


# --- Added at stage 8 -------------------------------------------------------

def test_a_missing_source_extract_is_an_error_not_an_empty_table(tmp_path, raw_dir):
    """An extract that failed to arrive must not read as an extract saying everything
    was deleted. `fx_rate` is loaded in full, so treating an absent file as zero rows
    would replace the raw table with an empty one - losing data because a job upstream
    did not run."""
    source = tmp_path / "source"
    write_source(source, FX_RATE, FX_ROWS)
    load.load_source(source, raw_dir, tables=["fx_rate"])
    assert raw.row_count(FX_RATE, raw_dir) == len(FX_ROWS)

    (source / "fx_rate.csv").unlink()
    with pytest.raises(FileNotFoundError, match="fx_rate"):
        load.load_source(source, raw_dir, tables=["fx_rate"])

    assert raw.row_count(FX_RATE, raw_dir) == len(FX_ROWS), "the raw table was emptied"


def test_a_missing_source_extract_is_a_usage_error_at_the_command(tmp_path, raw_dir):
    """The same code the command returns for a source directory that is not there."""
    source = tmp_path / "source"
    write_source(source, GL_ENTRY, SOURCE_ROWS)

    assert load.main(["--source", str(source), "--raw", str(raw_dir),
                      "--table", "fx_rate"]) == 2


def test_an_unusable_flag_is_a_usage_error(tmp_path, raw_dir):
    """argparse ends the process on a usage error, which is right when this is the
    process and wrong when it is a function something called - so main returns the code
    rather than letting SystemExit escape."""
    assert load.main(["--overlap-days", "not-a-number"]) == 2
    assert load.main(["--nonsense"]) == 2


def test_a_corrupt_watermark_file_fails_rather_than_silently_resetting(source, raw_dir):
    """Reading an unparseable state file as "no watermark" would quietly re-scan the
    whole source and report a successful run - the corruption would never surface, and
    the next run would inherit it."""
    run(source, raw_dir)
    (raw_dir / "_state" / "watermarks.json").write_text("{not json", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        run(source, raw_dir)


DIM_ACCOUNT = contracts.load("dim_account_src")
DIM_COST_CENTER = contracts.load("dim_cost_center_src")

ACCOUNT_ROWS = [
    {"account_code": "1000", "name": "Assets", "parent_code": "",
     "account_type": "asset", "effective_date": "2026-01-01"},
]
COST_CENTER_ROWS = [
    {"cc_code": "CC01", "name": "Ops", "dept_code": "D1", "effective_date": "2026-01-01"},
]


VENDOR_ROWS = [
    {"vendor_code": "V-0001", "name": "Northwind Office Supplies", "category": "office"},
]


def write_every_table(source_dir):
    write_source(source_dir, DIM_ACCOUNT, ACCOUNT_ROWS)
    write_source(source_dir, DIM_COST_CENTER, COST_CENTER_ROWS)
    write_source(source_dir, contracts.load("dim_vendor"), VENDOR_ROWS)
    write_source(source_dir, FX_RATE, FX_ROWS)
    write_source(source_dir, GL_ADJUSTMENT, [adjustment("A1", "1", "2026-01-15", "2026-01-25")])
    write_source(source_dir, GL_ENTRY, SOURCE_ROWS)
    return source_dir


def test_the_command_with_no_table_flag_loads_every_contract(tmp_path, raw_dir, capsys):
    """Case 37 in the shape the case is written in: no --table, so the run is every
    contract in `contracts.tables()` order. The scoped form is the test above; this is
    the one an operator actually types."""
    source = write_every_table(tmp_path / "source")

    assert load.main(["--source", str(source), "--raw", str(raw_dir)]) == 0

    printed = capsys.readouterr().out.splitlines()
    assert printed[0].startswith("run ")
    assert printed[1:] == [
        "dim_account_src: scanned 1, inserted 1, updated 0, evicted 0, partitions 1, "
        "watermark none -> none",
        "dim_cost_center_src: scanned 1, inserted 1, updated 0, evicted 0, partitions 1, "
        "watermark none -> none",
        "dim_vendor: scanned 1, inserted 1, updated 0, evicted 0, partitions 1, "
        "watermark none -> none",
        "fx_rate: scanned 2, inserted 2, updated 0, evicted 0, partitions 1, watermark none -> none",
        "gl_adjustment: scanned 1, inserted 1, updated 0, evicted 0, partitions 1, "
        "watermark none -> 2026-01-25",
        f"gl_entry: scanned 4, inserted 4, updated 0, evicted 0, partitions 3, "
        f"watermark none -> {HIGHEST_ARRIVAL}",
    ]


def test_a_table_with_no_contract_is_a_usage_error(tmp_path, raw_dir):
    """`--table no_such_table` reaches the contract loader, not the file system, so it
    comes back as a ContractError rather than a missing extract."""
    source = tmp_path / "source"
    write_source(source, GL_ENTRY, SOURCE_ROWS)

    assert load.main(["--source", str(source), "--raw", str(raw_dir),
                      "--table", "no_such_table"]) == 2


def test_a_truncated_source_file_is_refused(tmp_path, raw_dir):
    """A file with no header at all is a truncated extract, not an empty one. Reading it
    as zero rows is what makes it dangerous: `fx_rate` is loaded in full, so mirroring
    "no rows" would erase whatever raw already held."""
    source = tmp_path / "source"
    write_source(source, FX_RATE, FX_ROWS)
    load.load_source(source, raw_dir, tables=["fx_rate"])

    (source / "fx_rate.csv").write_text("", encoding="utf-8")
    with pytest.raises(load.SourceShapeError, match="truncated"):
        load.load_source(source, raw_dir, tables=["fx_rate"])

    assert raw.row_count(FX_RATE, raw_dir) == len(FX_ROWS), "the raw table was emptied"


def test_a_header_with_no_rows_is_a_genuinely_empty_extract(tmp_path, raw_dir):
    """The other side of the line above. A source that really has nothing to say still
    writes its header, and a full reload mirrors that - which is what full reload
    means."""
    source = tmp_path / "source"
    write_source(source, FX_RATE, FX_ROWS)
    load.load_source(source, raw_dir, tables=["fx_rate"])

    write_source(source, FX_RATE, [])
    report = load.load_source(source, raw_dir, tables=["fx_rate"])

    assert report.tables[0].rows_scanned == 0
    assert raw.row_count(FX_RATE, raw_dir) == 0


@pytest.mark.parametrize("shape", ["short", "long"])
def test_a_row_whose_field_count_is_wrong_is_refused_before_anything_is_written(
    source, raw_dir, shape
):
    """zip is silent both ways: it drops the columns past a short row's end, and it
    drops a long row's extra fields. The short one surfaces as a KeyError while writing
    a partition - by which time earlier partitions of the same batch are on disk."""
    columns = [spec["name"] for spec in GL_ENTRY["columns"]]
    values = [SOURCE_ROWS[1][column] for column in columns]
    malformed = values[:-1] if shape == "short" else values + ["extra"]

    with (source / "gl_entry.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        writer.writerow([SOURCE_ROWS[0][column] for column in columns])
        writer.writerow(malformed)

    with pytest.raises(load.SourceShapeError, match="fields"):
        run(source, raw_dir)

    assert raw.row_count(GL_ENTRY, raw_dir) == 0, "a partition was written anyway"


def test_a_source_missing_a_key_column_is_refused(source, raw_dir):
    """Not only the watermark column. A header without `entry_id` used to escape as a
    KeyError from the middle of building the batch."""
    columns = [spec["name"] for spec in GL_ENTRY["columns"] if spec["name"] != "entry_id"]
    with (source / "gl_entry.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        for row in SOURCE_ROWS:
            writer.writerow([row[column] for column in columns])

    with pytest.raises(load.SourceShapeError, match="entry_id"):
        run(source, raw_dir)


@pytest.mark.parametrize("state", ["[]", '"just a string"', "42"])
def test_a_state_file_that_is_not_an_object_is_refused(source, raw_dir, state):
    """`[]` is valid JSON and reaches `.items()` as an AttributeError, which is neither
    of the two exits the command promises."""
    run(source, raw_dir)
    (raw_dir / "_state" / "watermarks.json").write_text(state, encoding="utf-8")

    with pytest.raises(load.StateError, match="expected an object"):
        run(source, raw_dir)
    assert load.main(["--source", str(source), "--raw", str(raw_dir),
                      "--table", "gl_entry"]) == 2


def test_a_stored_watermark_that_is_not_a_date_is_refused_on_the_way_in(source, raw_dir):
    """Checked when the state file is read, before anything is written. Left to
    `lower_bound` it raises out of strptime halfway through a run."""
    run(source, raw_dir)
    (raw_dir / "_state" / "watermarks.json").write_text(
        '{"gl_entry": "not-a-date"}', encoding="utf-8"
    )

    with pytest.raises(load.StateError, match="not-a-date"):
        run(source, raw_dir)


@pytest.mark.parametrize("corrupt,expected", [
    ("{not json", "readable JSON"),
    ('{"gl_entry": "not-a-date"}', "not a date"),
])
def test_the_command_turns_a_bad_state_file_into_an_exit_code(source, raw_dir, corrupt, expected, capsys):
    """`main(argv) -> int` has to stay true whatever the state file holds. A stack trace
    out of a nightly job says less than an exit code and a line naming the file."""
    run(source, raw_dir)
    (raw_dir / "_state" / "watermarks.json").write_text(corrupt, encoding="utf-8")

    assert load.main(["--source", str(source), "--raw", str(raw_dir),
                      "--table", "gl_entry"]) == 2
    assert expected in capsys.readouterr().err


def test_the_command_turns_a_bad_source_shape_into_an_exit_code(source, raw_dir, capsys):
    """The library raises; the command explains and returns 2."""
    run(source, raw_dir)
    (source / "gl_entry.csv").write_text("", encoding="utf-8")

    assert load.main(["--source", str(source), "--raw", str(raw_dir),
                      "--table", "gl_entry"]) == 2
    assert "truncated" in capsys.readouterr().err


def test_a_full_reload_of_a_partitioned_table_drops_the_periods_it_no_longer_has(
    tmp_path, raw_dir
):
    """`raw.write_table` is the full-reload primitive, and on a partitioned table it has
    to remove the period directories the new batch does not carry - otherwise a shrunk
    reload leaves rows nothing accounts for."""
    raw.write_table(GL_ENTRY, raw_dir, SOURCE_ROWS, run_id=RUN_A)
    assert len(list((raw_dir / "gl_entry").glob("accounting_period=*"))) == 3

    raw.write_table(GL_ENTRY, raw_dir, SOURCE_ROWS[:1], run_id=RUN_A)

    assert [path.name for path in (raw_dir / "gl_entry").glob("accounting_period=*")] == [
        "accounting_period=2026-01"
    ]
    assert raw.row_count(GL_ENTRY, raw_dir) == 1


def test_a_batch_that_cannot_be_written_does_not_delete_what_is_there(raw_dir):
    """The new partitions go down before the stale ones come up. The other order would
    delete February and then raise while writing January, losing a period to a batch
    that never landed."""
    raw.write_table(GL_ENTRY, raw_dir, SOURCE_ROWS, run_id=RUN_A)
    before = raw.checksum(GL_ENTRY, raw_dir)

    incomplete = dict(entry("E9", "1", "2026-01-05", "2026-01-06"))
    del incomplete["doc_id"]
    with pytest.raises(KeyError):
        raw.write_table(GL_ENTRY, raw_dir, [incomplete], run_id=RUN_A)

    assert raw.checksum(GL_ENTRY, raw_dir) == before


def test_the_watermark_file_is_replaced_rather_than_rewritten_in_place(source, raw_dir, monkeypatch):
    """A half-written state file is worse than a missing one: missing means "start
    again", half-written means the next run raises on a file nobody edited."""
    run(source, raw_dir)
    before = (raw_dir / "_state" / "watermarks.json").read_text(encoding="utf-8")

    def refuses_to_move(source_path, destination):
        raise OSError("the move failed")

    monkeypatch.setattr(load.os, "replace", refuses_to_move)
    with pytest.raises(OSError):
        load.Watermarks.load(raw_dir).save()
    monkeypatch.undo()

    assert (raw_dir / "_state" / "watermarks.json").read_text(encoding="utf-8") == before
    assert sorted(p.name for p in (raw_dir / "_state").iterdir()) == [
        "runs.jsonl", "watermarks.json"
    ]


def test_a_source_that_lost_its_watermark_column_is_refused(source, raw_dir):
    """Without this the missing column reads as "" on every row, which is below any
    bound: the second run would report zero rows selected and change nothing, and an
    extract that had lost a column would look like an extract with no news in it."""
    run(source, raw_dir)

    without = [{k: v for k, v in row.items() if k != "posted_at"} for row in SOURCE_ROWS]
    columns = [name for name in (s["name"] for s in GL_ENTRY["columns"]) if name != "posted_at"]
    with (source / "gl_entry.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        for row in without:
            writer.writerow([row[column] for column in columns])

    with pytest.raises(load.SourceShapeError, match="posted_at"):
        run(source, raw_dir)


def test_a_corrupt_watermark_file_is_a_usage_error_at_the_command(source, raw_dir):
    """The library raises; `main` still owes its caller an integer. A stack trace out
    of a nightly job says less than an exit code and a line naming the file."""
    run(source, raw_dir)
    (raw_dir / "_state" / "watermarks.json").write_text("{not json", encoding="utf-8")

    assert load.main(["--source", str(source), "--raw", str(raw_dir),
                      "--table", "gl_entry"]) == 2


def test_a_full_reload_with_no_rows_leaves_no_periods_behind(raw_dir):
    """The empty edge of the shrinking reload: every period goes, not all but one."""
    raw.write_table(GL_ENTRY, raw_dir, SOURCE_ROWS, run_id=RUN_A)
    assert len(list((raw_dir / "gl_entry").glob("accounting_period=*"))) == 3

    raw.write_table(GL_ENTRY, raw_dir, [], run_id=RUN_A)

    assert list((raw_dir / "gl_entry").glob("accounting_period=*")) == []
    assert raw.row_count(GL_ENTRY, raw_dir) == 0


def test_the_help_flag_is_not_an_error(capsys):
    """argparse ends the process on --help, which is right when this is the process and
    wrong when it is a function something called. `main` returns the code either way,
    and for --help that code is zero."""
    assert load.main(["--help"]) == 0
    assert "--overlap-days" in capsys.readouterr().out


@pytest.mark.parametrize("value", ["not-a-number", "1.5", ""])
def test_an_overlap_window_that_is_not_a_whole_number_says_so(value, capsys):
    """argparse would otherwise print its generic "invalid value" line, which does not
    say what would have been acceptable."""
    assert load.main(["--overlap-days", value]) == 2
    assert "whole number of days" in capsys.readouterr().err


# --- the two run identifiers, through the load ------------------------------
#
# Cases 15-21, 22, 23b, 23c, 24, 26 and 26a of task.md. `_first_run_id` is the run
# that first landed the key and survives every rewrite; `_last_run_id` belongs to
# whichever run wrote the file. See docs/adr/0018.

RUN_A = "20260901T031500Z-aaaaaa"
RUN_B = "20260902T031500Z-bbbbbb"

# Per whole-table-replacement table: the rows the first run lands, and the row the
# second run adds. The key of each is what case 20 looks up.
FULL_RELOAD_ROWS = {
    "dim_vendor": (
        [{"vendor_code": "V-0001", "name": "Northwind Office Supplies",
          "category": "office"}],
        [{"vendor_code": "V-0002", "name": "Bluebird Materials", "category": "materials"}],
    ),
    "fx_rate": (
        [{"currency": "CNY", "rate_date": "2026-01-01", "rate_to_base": "1.000000"}],
        [{"currency": "EUR", "rate_date": "2026-01-01", "rate_to_base": "7.800000"}],
    ),
    "dim_account_src": (
        [{"account_code": "6001", "name": "Cost of sales", "parent_code": "",
          "account_type": "expense", "effective_date": "2026-01-01"}],
        [{"account_code": "6002", "name": "Freight", "parent_code": "6001",
          "account_type": "expense", "effective_date": "2026-01-01"}],
    ),
    "dim_cost_center_src": (
        [{"cc_code": "CC01", "name": "Sales North", "dept_code": "D1",
          "effective_date": "2026-01-01"}],
        [{"cc_code": "CC02", "name": "Sales South", "dept_code": "D1",
          "effective_date": "2026-01-01"}],
    ),
}


def stamped(raw_dir, contract=GL_ENTRY):
    """Every landed row, keyed by primary key, with the two identifiers on it."""
    rows = {}
    for path in raw.partitions(contract, raw_dir):
        for row in raw.read_partition(contract, path, metadata=True):
            rows[tuple(row[name] for name in contract["primary_key"])] = row
    return rows


def identifiers(raw_dir, contract=GL_ENTRY):
    return {key: (row["_first_run_id"], row["_last_run_id"])
            for key, row in stamped(raw_dir, contract).items()}


def test_a_first_load_stamps_the_same_run_on_every_row(source, raw_dir):
    """Case 14, through the load."""
    first = run(source, raw_dir).run_id

    assert set(identifiers(raw_dir).values()) == {(first, first)}


def test_an_updated_row_keeps_the_run_that_first_landed_it(source, raw_dir):
    """Case 15. The key arrived in the first run; the second run only restated it."""
    first = run(source, raw_dir).run_id
    write_source(source, GL_ENTRY, SOURCE_ROWS[:2] + [
        entry("E3", "1", "2026-03-15", INSIDE_THE_WINDOW, amount_dr="999.00"),
        SOURCE_ROWS[3],
    ])
    second = run(source, raw_dir).run_id

    assert identifiers(raw_dir)[("E3", "1")] == (first, second)
    assert stamped(raw_dir)[("E3", "1")]["amount_dr"] == "999.00"


def test_a_row_the_batch_did_not_touch_keeps_its_first_run(source, raw_dir):
    """Case 16. E4 shares the March partition with E3 and is rewritten along with it,
    which is what makes a single `run_id` column unable to answer where a row came
    from. E1 is in January, which this batch never opens."""
    first = run(source, raw_dir).run_id
    write_source(source, GL_ENTRY, SOURCE_ROWS[:2] + [
        entry("E3", "1", "2026-03-15", INSIDE_THE_WINDOW, amount_dr="999.00"),
        SOURCE_ROWS[3],
    ])
    second = run(source, raw_dir).run_id

    assert identifiers(raw_dir)[("E4", "1")] == (first, second)
    assert identifiers(raw_dir)[("E1", "1")] == (first, first)


def test_a_full_reload_does_not_restamp_the_first_run(source, raw_dir):
    """Case 17. `--full` is the one command that rewrites everything, and it is where a
    single-column design would erase the whole history in one go."""
    first = run(source, raw_dir).run_id
    second = run(source, raw_dir, full=True).run_id

    assert set(identifiers(raw_dir).values()) == {(first, second)}


def test_a_row_that_moves_period_carries_its_first_run_with_it(source, raw_dir):
    """Case 18. The row leaves one partition and lands in another; it is the same row,
    and the run that brought it has not changed."""
    first = run(source, raw_dir).run_id
    moved = entry("E4", "1", "2026-04-16", INSIDE_THE_WINDOW)
    write_source(source, GL_ENTRY, SOURCE_ROWS[:3] + [moved])
    second = run(source, raw_dir).run_id

    landed = stamped(raw_dir)[("E4", "1")]
    assert landed["accounting_date"] == "2026-04-16"
    assert (landed["_first_run_id"], landed["_last_run_id"]) == (first, second)


def test_the_rows_left_behind_by_an_eviction_keep_their_first_run(source, raw_dir):
    """Case 19. Evicting E4 rewrites the March partition, and E3 is still in it."""
    first = run(source, raw_dir).run_id
    write_source(source, GL_ENTRY, SOURCE_ROWS[:3] + [
        entry("E4", "1", "2026-04-16", INSIDE_THE_WINDOW),
    ])
    second = run(source, raw_dir).run_id

    assert identifiers(raw_dir)[("E3", "1")] == (first, second)


@pytest.mark.parametrize("table", sorted(FULL_RELOAD_ROWS))
def test_a_whole_table_replacement_keeps_the_first_run(tmp_path, raw_dir, table):
    """Case 20. All three of them: this path does not read the old file to merge, so it
    has to read it for this. A `prior_first_run_ids` that only worked for one table
    would pass if only `fx_rate` were tested."""
    contract = contracts.load(table)
    source = tmp_path / "source"
    existing, arriving = FULL_RELOAD_ROWS[table]

    write_source(source, contract, existing)
    first = load.load_source(source, raw_dir, tables=[table]).run_id
    write_source(source, contract, existing + arriving)
    second = load.load_source(source, raw_dir, tables=[table]).run_id

    landed = identifiers(raw_dir, contract)
    key = tuple(existing[0][name] for name in contract["primary_key"])
    newcomer = tuple(arriving[0][name] for name in contract["primary_key"])
    assert landed[key] == (first, second)
    assert landed[newcomer] == (second, second)


def test_a_whole_table_replacement_does_not_resurrect_a_removed_row(tmp_path, raw_dir):
    """Case 21. Reading the old file for its identifiers must not turn the read into a
    merge: a row the source dropped is gone, which is what `write_table` means."""
    source = tmp_path / "source"
    existing, arriving = FULL_RELOAD_ROWS["fx_rate"]

    write_source(source, FX_RATE, existing + arriving)
    load.load_source(source, raw_dir, tables=["fx_rate"])
    write_source(source, FX_RATE, arriving)
    load.load_source(source, raw_dir, tables=["fx_rate"])

    landed = identifiers(raw_dir, FX_RATE)
    assert tuple(existing[0][name] for name in FX_RATE["primary_key"]) not in landed
    assert len(landed) == len(arriving)


def test_merge_partition_will_not_run_without_a_run_identifier(raw_dir):
    """Case 22. The third write entry point, for the reason the other two have it."""
    path = raw.partition_path(raw_dir, GL_ENTRY, "2026-01")
    with pytest.raises(TypeError):
        load.merge_partition(GL_ENTRY, path, [entry("E1", "1", "2026-01-15", "2026-01-20")])


def test_merge_partition_carries_the_first_run_across(raw_dir):
    """Case 23b. Called directly, without the load around it: this is the read that has
    to ask for the metadata, and the failure it prevents is the whole partition being
    restamped as landed by the run that merely reopened it."""
    path = raw.partition_path(raw_dir, GL_ENTRY, "2026-01")
    raw.write_partition(GL_ENTRY, path, [
        entry("E1", "1", "2026-01-15", "2026-01-20"),
        entry("E2", "1", "2026-01-16", "2026-01-21"),
    ], run_id=RUN_A)

    load.merge_partition(GL_ENTRY, path, [
        entry("E2", "1", "2026-01-16", "2026-01-21", amount_dr="999.00"),
    ], run_id=RUN_B)

    landed = {row["entry_id"]: row for row in raw.read_partition(GL_ENTRY, path, metadata=True)}
    assert (landed["E1"]["_first_run_id"], landed["E1"]["_last_run_id"]) == (RUN_A, RUN_B)
    assert (landed["E2"]["_first_run_id"], landed["E2"]["_last_run_id"]) == (RUN_A, RUN_B)


def test_evict_moved_keys_keeps_the_first_run_on_the_rows_it_leaves(raw_dir):
    """Case 23c. The other read that has to ask for the metadata. The kept row is
    rewritten by a run that was only cleaning up after a different row."""
    january = raw.partition_path(raw_dir, GL_ENTRY, "2026-01")
    raw.write_partition(GL_ENTRY, january, [
        entry("E1", "1", "2026-01-15", "2026-01-20"),
        entry("E2", "1", "2026-01-16", "2026-01-21"),
    ], run_id=RUN_A)

    evicted, rewritten = load.evict_moved_keys(
        GL_ENTRY, raw_dir, {("E2", "1"): "2026-02"}, run_id=RUN_B
    )

    assert (evicted, rewritten) == (1, 1)
    kept = raw.read_partition(GL_ENTRY, january, metadata=True)
    assert [row["entry_id"] for row in kept] == ["E1"]
    assert (kept[0]["_first_run_id"], kept[0]["_last_run_id"]) == (RUN_A, RUN_B)


def test_three_runs_still_leave_the_row_count_and_checksum_alone(source, raw_dir):
    """Case 24. The acceptance criterion of the previous ticket, re-asserted now that
    every row carries metadata that changes between runs. If this fails, the
    identifiers reached the checksum."""
    measurements = []
    for _ in range(3):
        run(source, raw_dir)
        measurements.append(
            (raw.row_count(GL_ENTRY, raw_dir), raw.checksum(GL_ENTRY, raw_dir))
        )

    assert len(set(measurements)) == 1


def test_after_three_runs_the_identifiers_say_first_and_third(source, raw_dir):
    """Case 26. The other half of case 24: the checksum is unchanged, and the metadata
    outside it did move.

    Every row was landed by the first run. Which run last wrote it depends on whether
    its partition was reopened: March is inside the overlap window and is rewritten
    every time, January and February are not touched again. That asymmetry is
    docs/adr/0016 made visible - a batch rewrites the periods it touches and opens no
    others - so it is asserted rather than filtered out."""
    identities = [run(source, raw_dir).run_id for _ in range(3)]

    landed = identifiers(raw_dir)
    assert set(landed) == {("E1", "1"), ("E2", "1"), ("E3", "1"), ("E4", "1")}
    assert {key: value for key, value in landed.items() if key[0] in {"E3", "E4"}} == {
        ("E3", "1"): (identities[0], identities[2]),
        ("E4", "1"): (identities[0], identities[2]),
    }
    assert {key: value for key, value in landed.items() if key[0] in {"E1", "E2"}} == {
        ("E1", "1"): (identities[0], identities[0]),
        ("E2", "1"): (identities[0], identities[0]),
    }


def test_a_real_failure_inside_a_table_is_recorded_the_same_way(tmp_path, raw_dir):
    """Case 8, through a failure the load raises itself rather than a stand-in. The
    stand-in pins the re-raise; this pins that a genuine failure - one that happens
    after the run is already under way - reaches the record with the right table on
    it."""
    source = tmp_path / "source"
    write_source(source, GL_ENTRY, SOURCE_ROWS)

    with pytest.raises(FileNotFoundError):
        load.load_source(source, raw_dir, tables=["gl_entry", "gl_adjustment"])

    record = runs.RunLog(raw_dir).read()[0]
    assert record.status == "failed"
    assert record.failed_table == "gl_adjustment"
    assert "gl_adjustment" in record.error
    # gl_entry got as far as it did before the run failed, and the record says so.
    assert [table.table for table in record.tables] == ["gl_entry"]
    assert record.tables[0].rows_scanned == 4


def test_a_source_column_named_like_a_run_identifier_is_ignored(tmp_path, raw_dir):
    """An added column is a compatible change, so a source can grow one called
    `_first_run_id`. Carrying it into the batch would let an extract stamp a raw row
    with a run that never happened - and the checksum would not notice, because the
    identifiers are outside it by construction."""
    source = tmp_path / "source"
    source.mkdir(parents=True, exist_ok=True)
    columns = [spec["name"] for spec in GL_ENTRY["columns"]] + ["_first_run_id"]
    with (source / "gl_entry.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        for row in SOURCE_ROWS:
            writer.writerow([row[name] for name in columns[:-1]] + ["a-run-that-never-ran"])

    landed = load.load_source(source, raw_dir, tables=["gl_entry"]).run_id

    assert set(identifiers(raw_dir).values()) == {(landed, landed)}


def test_a_partitioned_whole_table_replacement_keeps_the_first_run(raw_dir):
    """Case 20, on the branch of `write_table` a contract does not reach today.
    `write_table` is the full-reload primitive and takes a partitioned table; nothing
    declares `partition_by` without a watermark, so the load never sends one down this
    branch - which is exactly why it needs a test of its own."""
    raw.write_table(GL_ENTRY, raw_dir, SOURCE_ROWS, run_id=RUN_A)
    raw.write_table(GL_ENTRY, raw_dir, SOURCE_ROWS, run_id=RUN_B)

    assert set(identifiers(raw_dir).values()) == {(RUN_A, RUN_B)}


def test_the_command_prints_the_run_identifier_first(source, raw_dir, capsys):
    """Case 26a. The identifier is how anything else about the run is looked up, so the
    command that produces it has to say what it was."""
    assert load.main(["--source", str(source), "--raw", str(raw_dir),
                      "--table", "gl_entry"]) == 0

    printed = capsys.readouterr().out.splitlines()
    assert printed[0].startswith("run ")

    run_id = printed[0].removeprefix("run ").strip()
    assert [record.run_id for record in runs.RunLog(raw_dir).read()] == [run_id]


# --- the new columns reach the raw layer -------------------------------------
#
# Cases 30 and 31 of task.md. Everything else in this module builds its rows by
# hand; these two go through the generator, because what they check is that adding a
# column to the source needs no change in the loader at all.

def test_the_vendor_columns_reach_the_raw_layer(tmp_path, raw_dir):
    """Case 30. The loader was not touched by this ticket: the contract declares the
    columns and they land, which is the whole claim."""
    from generator import Config, generate

    source = tmp_path / "source"
    generate(Config(seed=42, periods="2026-01:2026-02", entries_per_period=40, out_dir=source))
    load.load_source(source, raw_dir)

    landed = list(raw.read_table(GL_ENTRY, raw_dir))
    assert landed, "nothing landed"
    assert all("vendor_code" in row and "description" in row for row in landed)
    assert any(row["vendor_code"] for row in landed), "every vendor came back empty"
    assert all(row["description"] for row in landed)

    from_source = {
        row["entry_id"]: (row["vendor_code"], row["description"])
        for row in csv.DictReader((source / "gl_entry.csv").open(encoding="utf-8", newline=""))
    }
    for row in landed:
        assert (row["vendor_code"], row["description"]) == from_source[row["entry_id"]]


def test_dim_vendor_lands_as_a_whole_table(tmp_path, raw_dir):
    """Case 31. No watermark and no partition column, so it is one file - the same
    path the two dimensions and the rate table take."""
    from generator import Config, generate
    from generator import dimensions

    source = tmp_path / "source"
    generate(Config(seed=42, periods="2026-01:2026-01", entries_per_period=20, out_dir=source))
    load.load_source(source, raw_dir)

    contract = contracts.load("dim_vendor")
    assert [path.name for path in (raw_dir / "dim_vendor").iterdir()] == [PART_FILE]
    assert raw.row_count(contract, raw_dir) == dimensions.VENDOR_COUNT
