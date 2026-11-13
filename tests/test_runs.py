"""The run record: what a run did, written before the run is over.

Two lines per run, appended and never rewritten. The `started` event records the
intent - this run, these tables, this window - and the `finished` event records what
came of it. The pair is what answers the morning question: something started at
03:15, said it would read these six tables, and never said anything again. A record
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
DIM_VENDOR = contracts.load("dim_vendor")
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
VENDORS = [{"vendor_code": "V-0001", "name": "Northwind Office Supplies",
            "category": "office"}]

EVERY_TABLE = {
    GL_ENTRY["table"]: (GL_ENTRY, ENTRIES),
    GL_ADJUSTMENT["table"]: (GL_ADJUSTMENT, ADJUSTMENTS),
    DIM_ACCOUNT["table"]: (DIM_ACCOUNT, ACCOUNTS),
    DIM_COST_CENTER["table"]: (DIM_COST_CENTER, COST_CENTERS),
    DIM_VENDOR["table"]: (DIM_VENDOR, VENDORS),
    FX_RATE["table"]: (FX_RATE, RATES),
}


@pytest.fixture
def raw_dir(tmp_path):
    return tmp_path / "raw"


@pytest.fixture
def source(tmp_path):
    """A source directory holding all six tables, so one run touches both the
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


def test_every_table_records_the_digest_of_the_file_it_read(source, raw_dir):
    """Case 7. All six tables, incremental and whole-table replacement alike - this is
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


def test_a_corrupt_log_is_a_usage_error_at_the_command(source, raw_dir, capsys):
    """Case 31. The library raises; the command is what turns that into an exit code -
    the same division of labour as `ingest.validate`. See docs/adr/0012."""
    load.load_source(source, raw_dir, tables=["gl_entry"])
    path = raw_dir / "_state" / "runs.jsonl"
    corrupt = len(path.read_text(encoding="utf-8").splitlines()) + 1
    with path.open("a", encoding="utf-8") as handle:
        handle.write("{ not json\n")

    assert runs.main(["--raw", str(raw_dir)]) == 2
    # `line N`, not a bare digit: a bare one matches the digits of a temporary path,
    # which is how this passed locally and failed in CI.
    assert f"line {corrupt}" in capsys.readouterr().err


# --- the record over steps -------------------------------------------------
#
# A run is a sequence of named steps, and the record holds that sequence. The step
# events are docs/adr/0019's ordering argument applied one level down: what is written
# first is what survives the failure, so a run that died at `facts` leaves a
# `step_started` for `facts` and nothing after it. See docs/adr/0044.
#
# Cases 1-8 of orchestrate-the-daily-run.

STEPS = ["validate", "load", "recompute", "mart-load", "dbt-build", "clear-affected"]


def open_run(log, run_id, *, steps=STEPS, command="daily", **extra):
    """A `started` event for a pipeline run, without going through the runner."""
    log.start(run_id, command=command, source="data/source", raw="data/raw",
              tables=[], steps=steps, **extra)


def walk(log, run_id, done, *, status="succeeded", detail=None):
    """Record `done` as completed steps, so a test can set up the run it is about."""
    for step in done:
        log.step_started(run_id, step)
        log.step_finished(run_id, step, status=status, duration_seconds=0.5,
                          detail=detail or {})


def steps_of(record):
    return [(step.step, step.status) for step in record.steps]


def test_a_run_folds_into_the_steps_it_ran(raw_dir):
    """Case 1. Three step pairs under one run become three StepRuns, in log order,
    each carrying its status and duration."""
    log = runs.RunLog(raw_dir)
    run_id = runs.new_run_id()
    open_run(log, run_id)
    walk(log, run_id, ["validate", "load", "recompute"])
    log.finish(run_id, status="succeeded", duration_seconds=9.0, tables=[])

    recorded = log.read()

    assert len(recorded) == 1
    assert steps_of(recorded[0]) == [
        ("validate", "succeeded"), ("load", "succeeded"), ("recompute", "succeeded"),
    ]
    assert [step.duration_seconds for step in recorded[0].steps] == [0.5, 0.5, 0.5]


def test_an_interrupted_run_names_the_step_it_stopped_in(raw_dir):
    """Case 2. A `step_started` with no `step_finished` and no `finished` event: the
    absence of the line is what says where it died."""
    log = runs.RunLog(raw_dir)
    run_id = runs.new_run_id()
    open_run(log, run_id)
    walk(log, run_id, ["validate", "load"])
    log.step_started(run_id, "recompute")

    record = log.read()[0]

    assert record.status == runs.INTERRUPTED
    assert record.unfinished_step == "recompute"
    assert steps_of(record)[-1] == ("recompute", runs.INTERRUPTED)


def test_a_failed_run_names_the_step_and_keeps_the_ones_before_it(raw_dir):
    """Case 3. The steps that worked are still recorded as having worked - a record
    that only said the run failed would lose what it managed to do first."""
    log = runs.RunLog(raw_dir)
    run_id = runs.new_run_id()
    open_run(log, run_id)
    walk(log, run_id, ["validate", "load"])
    log.step_started(run_id, "recompute")
    log.step_finished(run_id, "recompute", status="failed", duration_seconds=1.0,
                      detail={})
    log.finish(run_id, status="failed", duration_seconds=4.0, tables=[],
               failed_step="recompute", error="no such period 2026-13")

    record = log.read()[0]

    assert record.status == "failed"
    assert record.failed_step == "recompute"
    assert record.error == "no such period 2026-13"
    assert steps_of(record) == [
        ("validate", "succeeded"), ("load", "succeeded"), ("recompute", "failed"),
    ]


def test_a_step_starting_again_after_it_finished_is_a_retry(raw_dir):
    """Case 5. A step can be re-run under the run identifier it already has, so a
    retried step really does start twice inside one run. Both attempts are kept: that is
    what happened. See docs/adr/0045."""
    log = runs.RunLog(raw_dir)
    run_id = runs.new_run_id()
    open_run(log, run_id)
    log.step_started(run_id, "load")
    log.step_finished(run_id, "load", status="failed", duration_seconds=1.0, detail={})
    log.step_started(run_id, "load")
    log.step_finished(run_id, "load", status="succeeded", duration_seconds=2.0,
                      detail={})

    record = log.read()[0]

    assert steps_of(record) == [("load", "failed"), ("load", "succeeded")]
    # Numbered by the reader, never by the writer: a retried step may run in a new
    # process and have no idea it is the second try, so one that wrote its own number
    # would write a second attempt 1.
    assert [step.attempt for step in record.steps] == [1, 2]


def test_a_step_event_for_a_run_that_never_started_is_a_corrupt_log(raw_dir):
    """The log is appended to and never edited, so this means it was truncated or
    written by something else - reading past it would report a history that is not what
    happened."""
    log = runs.RunLog(raw_dir)
    log.step_started(runs.new_run_id(), "load")

    with pytest.raises(runs.RunLogError, match="never started"):
        log.read()


