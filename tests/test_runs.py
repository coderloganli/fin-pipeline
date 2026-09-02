"""The run record: what a run did, written before the run is over.

Two lines per run, appended and never rewritten. The `started` event records the
intent - this run, these tables, this window - and the `finished` event records what
came of it. The pair is what answers the morning question: something started at
03:15, said it would read these five tables, and never said anything again. A record
written only on the paths that worked would be missing from exactly the run somebody
is investigating. See docs/adr/0019.

The source file digest and the ingestion time live here rather than on the row: they
are facts about a run, not about a row, and stamping them onto every row would repeat
one value several million times. See docs/adr/0020.

Cases 1-13 and 27-31 of task.md.
"""

import hashlib
import json
import re
from datetime import datetime, timezone

import pytest

from ingest import contracts, load, runs

# Imported as a bare module name, the way conftest is: pytest puts the tests directory
# on sys.path. See the note in tests/test_long_tail.py.
from test_load import GL_ADJUSTMENT, GL_ENTRY, adjustment, entry, write_source

DIM_ACCOUNT = contracts.load("dim_account_src")
DIM_COST_CENTER = contracts.load("dim_cost_center_src")
FX_RATE = contracts.load("fx_rate")

RUN_ID = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{6}$")

ENTRIES = [
    entry("E1", "1", "2026-01-15", "2026-01-20"),
    entry("E2", "1", "2026-02-15", "2026-02-20"),
]
ADJUSTMENTS = [adjustment("A1", "1", "2026-01-15", "2026-01-25")]
ACCOUNTS = [{
    "account_code": "6001", "name": "Cost of sales", "parent_code": "",
    "account_type": "expense", "effective_date": "2026-01-01",
}]
COST_CENTERS = [{
    "cc_code": "CC01", "name": "Sales North", "dept_code": "D1",
    "effective_date": "2026-01-01",
}]
RATES = [{"currency": "CNY", "rate_date": "2026-01-01", "rate_to_base": "1.000000"}]

EVERY_TABLE = {
    GL_ENTRY["table"]: (GL_ENTRY, ENTRIES),
    GL_ADJUSTMENT["table"]: (GL_ADJUSTMENT, ADJUSTMENTS),
    DIM_ACCOUNT["table"]: (DIM_ACCOUNT, ACCOUNTS),
    DIM_COST_CENTER["table"]: (DIM_COST_CENTER, COST_CENTERS),
    FX_RATE["table"]: (FX_RATE, RATES),
}


@pytest.fixture
def raw_dir(tmp_path):
    return tmp_path / "raw"


@pytest.fixture
def source(tmp_path):
    """A source directory holding all five tables, so one run touches both the
    incremental path and the whole-table replacement one."""
    directory = tmp_path / "source"
    for contract, rows in EVERY_TABLE.values():
        write_source(directory, contract, rows)
    return directory


def lines(raw_dir):
    path = raw_dir / "_state" / "runs.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def digest_of(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def by_table(record):
    return {table.table: table for table in record.tables}


# --- the record ------------------------------------------------------------

def test_a_successful_load_leaves_one_record(source, raw_dir):
    """Case 1."""
    report = load.load_source(source, raw_dir)

    recorded = runs.RunLog(raw_dir).read()
    assert len(recorded) == 1
    assert recorded[0].status == "succeeded"
    assert recorded[0].run_id == report.run_id
    assert RUN_ID.match(recorded[0].run_id)


def test_the_log_holds_a_started_and_a_finished_event(source, raw_dir):
    """Case 2. Two lines, in that order: what is written first is what survives the
    failure."""
    load.load_source(source, raw_dir)

    written = lines(raw_dir)
    assert [event["event"] for event in written] == ["started", "finished"]
    assert written[0]["run_id"] == written[1]["run_id"]
    assert sorted(written[0]["tables"]) == sorted(EVERY_TABLE)


def test_two_identifiers_made_in_the_same_second_differ(monkeypatch):
    """Case 3. The timestamp is only to the second; the suffix is what keeps two runs
    started inside one second apart."""
    frozen = datetime(2026, 9, 1, 3, 15, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(runs, "now", lambda: frozen)

    first, second = runs.new_run_id(), runs.new_run_id()

    assert first != second
    assert RUN_ID.match(first) and RUN_ID.match(second)
    assert first[:16] == second[:16]


def test_runs_are_read_in_log_order_rather_than_identifier_order(raw_dir, monkeypatch):
    """Case 4. Chronological order comes from the append-only file. Two runs started in
    the same second sort arbitrarily by identifier, so sorting them would reorder the
    log - here the second run's suffix sorts before the first's."""
    frozen = datetime(2026, 9, 1, 3, 15, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(runs, "now", lambda: frozen)
    suffixes = iter(["ffffff", "000000"])
    monkeypatch.setattr(runs, "token", lambda: next(suffixes))

    log = runs.RunLog(raw_dir)
    written = []
    for _ in range(2):
        run_id = runs.new_run_id()
        written.append(run_id)
        log.start(run_id, command="load", source="data/source", raw=str(raw_dir),
                  tables=["gl_entry"], overlap_days=7, full=False)
        log.finish(run_id, status="succeeded", duration_seconds=0.0, tables=[])

    assert written != sorted(written)
    assert [record.run_id for record in log.read()] == written


def test_the_duration_and_the_two_timestamps_are_recorded(source, raw_dir):
    """Case 5."""
    load.load_source(source, raw_dir)

    record = runs.RunLog(raw_dir).read()[0]
    assert record.duration_seconds >= 0
    assert record.started_at <= record.finished_at


def test_each_table_records_what_the_load_report_says_it_did(source, raw_dir):
    """Case 6. The record and the summary line describe the same run, so every field
    they share has to agree."""
    report = load.load_source(source, raw_dir)

    recorded = by_table(runs.RunLog(raw_dir).read()[0])
    assert set(recorded) == {table.table for table in report.tables}
    for table in report.tables:
        written = recorded[table.table]
        assert written.rows_scanned == table.rows_scanned
        assert written.rows_inserted == table.rows_inserted
        assert written.rows_updated == table.rows_updated
        assert written.rows_evicted == table.rows_evicted
        assert written.partitions_written == table.partitions_written
        assert written.watermark_from == table.watermark_from
        assert written.watermark_to == table.watermark_to


def test_every_table_records_the_digest_of_the_file_it_read(source, raw_dir):
    """Case 7. All five tables, incremental and whole-table replacement alike - this is
    the only answer to whether the extract ingested was the bytes that were sent."""
    report = load.load_source(source, raw_dir)

    recorded = by_table(runs.RunLog(raw_dir).read()[0])
    assert set(recorded) == set(EVERY_TABLE)
    for name in EVERY_TABLE:
        assert recorded[name].source_sha256 == digest_of(source / f"{name}.csv")

    for table in report.tables:
        assert table.source_sha256 == recorded[table.table].source_sha256


def test_a_run_that_fails_names_the_table_and_still_raises(source, raw_dir, monkeypatch):
    """Case 8. The record does not swallow the failure: the caller's exit code is
    decided the way it was before any of this existed."""
    real = load.load_table

    def fails_on_the_adjustments(contract, *args, **kwargs):
        if contract["table"] == "gl_adjustment":
            raise RuntimeError("the disk went away")
        return real(contract, *args, **kwargs)

    monkeypatch.setattr(load, "load_table", fails_on_the_adjustments)

    with pytest.raises(RuntimeError, match="the disk went away"):
        load.load_source(source, raw_dir, tables=["gl_entry", "gl_adjustment"])

    record = runs.RunLog(raw_dir).read()[0]
    assert record.status == "failed"
    assert record.failed_table == "gl_adjustment"
    assert "the disk went away" in record.error


def test_a_run_with_no_finished_event_reads_as_interrupted(raw_dir):
    """Case 9. What a run killed outright leaves behind. Saying `interrupted` is the
    true statement about it; a run still going looks identical, by design."""
    log = runs.RunLog(raw_dir)
    run_id = runs.new_run_id()
    log.start(run_id, command="load", source="data/source", raw=str(raw_dir),
              tables=["gl_entry"], overlap_days=7, full=False)

    record = log.read()[0]
    assert record.status == "interrupted"
    assert record.finished_at is None
    assert record.duration_seconds is None


def test_three_loads_leave_three_records_in_order(source, raw_dir):
    """Case 10."""
    identifiers = [load.load_source(source, raw_dir).run_id for _ in range(3)]

    recorded = runs.RunLog(raw_dir).read()
    assert [record.run_id for record in recorded] == identifiers
    assert len(set(identifiers)) == 3


def test_a_run_that_selects_nothing_is_still_recorded(source, raw_dir):
    """Case 11. A run that had nothing to do is a run that happened, and the record is
    how anyone knows the pipeline was alive at all."""
    load.load_source(source, raw_dir, tables=["gl_entry"])
    write_source(source, GL_ENTRY, ENTRIES[:1])
    load.load_source(source, raw_dir, tables=["gl_entry"])

    second = runs.RunLog(raw_dir).read()[1]
    assert second.status == "succeeded"
    assert by_table(second)["gl_entry"].rows_scanned == 0


def test_an_unreadable_line_names_its_line_number(source, raw_dir):
    """Case 12. Not skipped: a log that quietly drops what it cannot parse reports a
    history missing the very run somebody is looking for."""
    load.load_source(source, raw_dir, tables=["gl_entry"])
    path = raw_dir / "_state" / "runs.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write("this is not JSON\n")

    with pytest.raises(runs.RunLogError, match="3"):
        runs.RunLog(raw_dir).read()


def test_one_identifier_starting_twice_is_a_corrupt_log(source, raw_dir):
    """One identifier cannot mean two runs. Six hex characters inside one second make a
    collision negligible, so this is a log that was concatenated or edited, and reading
    on would hand the caller an ambiguous history."""
    load.load_source(source, raw_dir, tables=["gl_entry"])
    log = runs.RunLog(raw_dir)
    repeated = log.read()[0].run_id
    log.start(repeated, command="load", source="data/source", raw=str(raw_dir),
              tables=["gl_entry"], overlap_days=7, full=False)

    with pytest.raises(runs.RunLogError, match="starts twice"):
        log.read()


def test_one_identifier_finishing_twice_is_a_corrupt_log(source, raw_dir):
    """The same reasoning as a repeated start. A second finished event would silently
    replace the first run's outcome, and nothing here can decide which is true."""
    load.load_source(source, raw_dir, tables=["gl_entry"])
    log = runs.RunLog(raw_dir)
    finished = log.read()[0].run_id
    log.finish(finished, status="failed", duration_seconds=1.0, tables=[])

    with pytest.raises(runs.RunLogError, match="finishes twice"):
        log.read()


def test_a_finished_run_cannot_claim_to_be_interrupted(raw_dir):
    """`interrupted` is what the absence of a finished event means, so a finished event
    saying it would be a run claiming to have finished by not finishing."""
    log = runs.RunLog(raw_dir)
    run_id = runs.new_run_id()
    log.start(run_id, command="load", source="data/source", raw=str(raw_dir),
              tables=["gl_entry"], overlap_days=7, full=False)

    with pytest.raises(ValueError, match="succeeded or failed"):
        log.finish(run_id, status="interrupted", duration_seconds=1.0, tables=[])


def test_the_state_directory_is_created_when_it_is_missing(source, raw_dir):
    """Case 13."""
    assert not raw_dir.exists()

    load.load_source(source, raw_dir, tables=["gl_entry"])

    assert (raw_dir / "_state" / "runs.jsonl").is_file()


# --- the command -----------------------------------------------------------

def test_the_command_lists_the_runs_most_recent_first(source, raw_dir, capsys):
    """Case 27."""
    first = load.load_source(source, raw_dir, tables=["gl_entry"]).run_id
    second = load.load_source(source, raw_dir, tables=["gl_entry"]).run_id

    assert runs.main(["--raw", str(raw_dir)]) == 0

    printed = capsys.readouterr().out
    assert first in printed and second in printed
    assert printed.index(second) < printed.index(first)
    assert "succeeded" in printed


def test_the_command_shows_one_run_in_full(source, raw_dir, capsys):
    """Case 28. What the acceptance criterion asks for: which data this run handled,
    and how long it took."""
    run_id = load.load_source(source, raw_dir).run_id

    assert runs.main(["--raw", str(raw_dir), "--run", run_id]) == 0

    printed = capsys.readouterr().out
    assert run_id in printed
    recorded = by_table(runs.RunLog(raw_dir).read()[0])
    for name in EVERY_TABLE:
        assert recorded[name].describe() in printed
        assert digest_of(source / f"{name}.csv") in printed
    assert "2026-02-20" in printed  # the watermark this run reached
    assert "duration" in printed


def test_an_unknown_run_is_a_usage_error(source, raw_dir, capsys):
    """Case 29."""
    load.load_source(source, raw_dir, tables=["gl_entry"])

    assert runs.main(["--raw", str(raw_dir), "--run", "20260101T000000Z-abcdef"]) == 2
    assert "20260101T000000Z-abcdef" in capsys.readouterr().err


def test_the_command_says_so_when_there_are_no_runs(raw_dir, capsys):
    """Case 30. Nothing recorded yet is not a failure."""
    assert runs.main(["--raw", str(raw_dir)]) == 0
    assert "no runs" in capsys.readouterr().out.lower()


def test_a_corrupt_log_is_a_usage_error_at_the_command(source, raw_dir, capsys):
    """Case 31. The library raises; the command is what turns that into an exit code -
    the same division of labour as `ingest.validate`. See docs/adr/0012."""
    load.load_source(source, raw_dir, tables=["gl_entry"])
    path = raw_dir / "_state" / "runs.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write("{ not json\n")

    assert runs.main(["--raw", str(raw_dir)]) == 2
    assert "3" in capsys.readouterr().err
