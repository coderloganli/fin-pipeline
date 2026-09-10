"""The affected-period set: what a run records, and what recomputes from it.

Two triggers, not one. An entry landing in a period dirties it, and a dimension version
taking effect over a period dirties it too - which is the gap docs/adr/0027 recorded and
left open, because a dimension change produces no entry and so triggered no
recomputation at all.

The set is written before the watermark moves. The reverse order loses the signal
exactly where it is needed: a crash between an advanced watermark and an unwritten set
means the next run does not re-read those source rows and never learns the periods were
owed. Case 8 is what pins the ordering.

See docs/adr/0039, 0040 and 0041. Cases 1-16 of task.md.
"""

import pytest

from ingest import affected, contracts, load, raw
from transform.spark import affected as resolve_affected
from transform.spark import balances, facts, scd2

GL_ENTRY = contracts.load("gl_entry")
DIM_CC = contracts.load("dim_cost_center_src")
DIM_ACCOUNT = contracts.load("dim_account_src")
DIM_FX = contracts.load("fx_rate")

RUN_A = "20260901T031500Z-aaaaaa"

EXPENSE = {"account_code": "660204", "name": "Office supplies", "parent_code": "6602",
           "account_type": "expense", "effective_date": "2020-01-01"}
CENTRE = {"cc_code": "CC-001", "name": "Ops", "dept_code": "DEPT-OPS",
          "effective_date": "2020-01-01"}
RATE = {"currency": "CNY", "rate_date": "2026-01-01", "rate_to_base": "1.000000"}


def entry_row(entry_id, accounting_date, *, version="1", posted_at=None,
              account="660204", centre="CC-001", currency="CNY", dr="100.00"):
    return {
        "entry_id": entry_id, "version": version,
        "accounting_date": accounting_date, "posted_at": posted_at or accounting_date,
        "account_code": account, "cost_center_code": centre,
        "currency": currency, "amount_dr": dr, "amount_cr": "0.00",
        "doc_id": f"DOC-{entry_id}", "vendor_code": "", "description": "x",
    }


def write_source(source_dir, table: str, rows, contract):
    """One source extract, in the declared column order."""
    import csv

    source_dir.mkdir(parents=True, exist_ok=True)
    columns = raw.columns_of(contract)
    with (source_dir / f"{table}.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row[name] for name in columns})


def full_source(source_dir, *, entries, centres=(CENTRE,), accounts=(EXPENSE,),
                rates=(RATE,), adjustments=()):
    write_source(source_dir, "gl_entry", entries, GL_ENTRY)
    write_source(source_dir, "gl_adjustment", adjustments, contracts.load("gl_adjustment"))
    write_source(source_dir, "dim_cost_center_src", centres, DIM_CC)
    write_source(source_dir, "dim_account_src", accounts, DIM_ACCOUNT)
    write_source(source_dir, "fx_rate", rates, DIM_FX)
    write_source(source_dir, "dim_vendor", [], contracts.load("dim_vendor"))


# --- what a run records -----------------------------------------------------


def test_a_merge_records_the_period_it_wrote(tmp_path):
    """Case 1."""
    source, raw_dir = tmp_path / "source", tmp_path / "raw"
    full_source(source, entries=[entry_row("E-1", "2026-03-04")])

    load.load_source(source, raw_dir)

    assert affected.read(raw_dir).periods == ["2026-03"]


def test_two_runs_union_rather_than_replace(tmp_path):
    """Case 2."""
    source, raw_dir = tmp_path / "source", tmp_path / "raw"
    full_source(source, entries=[entry_row("E-1", "2026-03-04")])
    load.load_source(source, raw_dir)

    full_source(source, entries=[entry_row("E-1", "2026-03-04"),
                                 entry_row("E-2", "2026-05-06")])
    load.load_source(source, raw_dir)

    assert set(affected.read(raw_dir).periods) == {"2026-03", "2026-05"}


def test_a_run_that_changes_nothing_records_nothing(tmp_path):
    """Case 3. `merge_table` counts an update only when a declared column differs, so a
    nightly extract re-presenting history it has already landed reports nothing."""
    source, raw_dir = tmp_path / "source", tmp_path / "raw"
    full_source(source, entries=[entry_row("E-1", "2026-03-04")])
    load.load_source(source, raw_dir)
    affected.clear(raw_dir)

    load.load_source(source, raw_dir, full=True)

    assert affected.read(raw_dir).periods == []


def test_a_new_dimension_version_is_recorded_as_a_version(tmp_path):
    """Case 4. A dimension change produces no entry, so it records no period."""
    source, raw_dir = tmp_path / "source", tmp_path / "raw"
    full_source(source, entries=[entry_row("E-1", "2026-03-04")])
    load.load_source(source, raw_dir)
    affected.clear(raw_dir)

    moved = {**CENTRE, "dept_code": "DEPT-FIN", "effective_date": "2026-04-01"}
    full_source(source, entries=[entry_row("E-1", "2026-03-04")], centres=(CENTRE, moved))
    load.load_source(source, raw_dir)

    state = affected.read(raw_dir)
    assert state.periods == []
    assert state.dimension_versions == [
        {"table": "dim_cost_center_src", "key": ["CC-001"], "effective_date": "2026-04-01"}
    ]


def test_a_re_presented_dimension_version_is_not_recorded(tmp_path):
    """Case 5."""
    source, raw_dir = tmp_path / "source", tmp_path / "raw"
    moved = {**CENTRE, "dept_code": "DEPT-FIN", "effective_date": "2026-04-01"}
    full_source(source, entries=[entry_row("E-1", "2026-03-04")], centres=(CENTRE, moved))
    load.load_source(source, raw_dir)
    affected.clear(raw_dir)

    load.load_source(source, raw_dir)

    assert affected.read(raw_dir).dimension_versions == []


def test_an_entry_that_moves_period_dirties_both(tmp_path):
    """Case 6. The partition it left changed too, so `evict_moved_keys` has to return
    the periods it rewrote rather than only a count."""
    source, raw_dir = tmp_path / "source", tmp_path / "raw"
    full_source(source, entries=[entry_row("E-1", "2026-03-04")])
    load.load_source(source, raw_dir)
    affected.clear(raw_dir)

    # The same version, re-dated. A version bump would leave the March row in place -
    # raw never forgets a primary key - so March would genuinely not have changed. It is
    # the re-date without a bump that moves a row between partitions, which is the case
    # `evict_moved_keys` exists for.
    full_source(source, entries=[
        entry_row("E-1", "2026-05-06", version="1", posted_at="2026-05-06")
    ])
    load.load_source(source, raw_dir, full=True)

    assert set(affected.read(raw_dir).periods) == {"2026-03", "2026-05"}


def test_clear_empties_the_file_and_a_missing_file_reads_empty(tmp_path):
    """Case 7."""
    raw_dir = tmp_path / "raw"
    assert affected.read(raw_dir).periods == []
    assert affected.read(raw_dir).dimension_versions == []

    affected.record(raw_dir, periods=["2026-03"], versions=[])
    assert affected.read(raw_dir).periods == ["2026-03"]

    affected.clear(raw_dir)
    assert affected.read(raw_dir).periods == []


def test_a_full_load_records_every_period_it_rebuilt(tmp_path):
    """Case 7a. `--full` ignores the watermark, so what it rebuilt is every period it
    landed - not the window a watermark would have chosen.

    Both rows are changed, and one of them was posted four months before the stored
    watermark, so a watermarked run would never read it. That is the contrast the case
    is about, and it is asserted both ways.
    """
    original = [entry_row("E-1", "2026-03-04"), entry_row("E-2", "2026-07-08")]
    changed = [entry_row("E-1", "2026-03-04", dr="999.00"),
               entry_row("E-2", "2026-07-08", dr="999.00")]

    def landed(name: str, *, full: bool) -> set[str]:
        """One raw layer per branch: applying the change once would leave the second
        branch with nothing left to change, and the comparison would say nothing."""
        source, raw_dir = tmp_path / f"source-{name}", tmp_path / f"raw-{name}"
        full_source(source, entries=original)
        load.load_source(source, raw_dir)
        full_source(source, entries=changed)
        affected.clear(raw_dir)
        load.load_source(source, raw_dir, full=full)
        return set(affected.read(raw_dir).periods)

    assert landed("windowed", full=False) == {"2026-07"}, \
        "the watermark window cannot reach March"
    assert landed("full", full=True) == {"2026-03", "2026-07"}


# --- crash boundaries -------------------------------------------------------
#
# Case 8. Three injection points. Only (c) distinguishes the ordering this design
# depends on; the other two establish that the set is not lost in the ordinary
# interruptions either.


def two_period_source(tmp_path):
    source, raw_dir = tmp_path / "source", tmp_path / "raw"
    full_source(source, entries=[entry_row("E-1", "2026-03-04"),
                                 entry_row("E-2", "2026-04-05")])
    return source, raw_dir


def test_a_crash_writing_a_partition_leaves_the_work_owed(tmp_path, monkeypatch):
    """Case 8a. Neither the watermark nor the set moved; the next run re-reads the
    window and records both."""
    source, raw_dir = two_period_source(tmp_path)
    real = raw.write_partition
    calls = {"n": 0}

    def failing(contract, path, rows, *, run_id):
        if contract["table"] == "gl_entry":
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("interrupted")
        return real(contract, path, rows, run_id=run_id)

    monkeypatch.setattr(raw, "write_partition", failing)
    with pytest.raises(RuntimeError):
        load.load_source(source, raw_dir)
    monkeypatch.undo()

    load.load_source(source, raw_dir)
    assert set(affected.read(raw_dir).periods) == {"2026-03", "2026-04"}


def test_a_crash_saving_the_watermark_leaves_the_set_written(tmp_path, monkeypatch):
    """Case 8b. The set holds both periods and the watermark did not move, so the next
    run re-reads and the union leaves the set unchanged."""
    source, raw_dir = two_period_source(tmp_path)

    def failing(self):
        raise RuntimeError("interrupted")

    monkeypatch.setattr(load.Watermarks, "save", failing)
    with pytest.raises(RuntimeError):
        load.load_source(source, raw_dir)
    monkeypatch.undo()

    assert set(affected.read(raw_dir).periods) == {"2026-03", "2026-04"}

    load.load_source(source, raw_dir)
    assert set(affected.read(raw_dir).periods) == {"2026-03", "2026-04"}


def test_the_set_is_written_before_the_watermark(tmp_path, monkeypatch):
    """Case 8c. The ordering this test exists to pin: after the watermark has been
    saved the set already holds both periods. Reversing the two writes loses them, and
    the watermark has moved so nothing re-reads those rows."""
    source, raw_dir = two_period_source(tmp_path)
    real = load.Watermarks.save

    def failing(self):
        real(self)
        raise RuntimeError("interrupted after the watermark")

    monkeypatch.setattr(load.Watermarks, "save", failing)
    with pytest.raises(RuntimeError):
        load.load_source(source, raw_dir)
    monkeypatch.undo()

    assert set(affected.read(raw_dir).periods) == {"2026-03", "2026-04"}


# --- the closure ------------------------------------------------------------


def test_a_dirty_period_drags_its_windows_with_it():
    """Case 9. Month-on-month reaches M+1, the three-month rolling mean M+2, and
    year-on-year M+12."""
    assert balances.dirty_closure({"2026-03"}, last_period="2027-12") == {
        "2026-03", "2026-04", "2026-05", "2027-03",
    }


def test_the_closure_is_clipped_to_the_reporting_range():
    """Case 10."""
    assert balances.dirty_closure({"2026-03"}, last_period="2026-06") == {
        "2026-03", "2026-04", "2026-05",
    }


def test_the_closure_is_derived_from_the_window_constants(monkeypatch):
    """Case 11. Widening the rolling mean widens the closure, because the closure reads
    the constant rather than restating it."""
    monkeypatch.setattr(balances, "ROLLING_SPAN", 6)
    closure = balances.dirty_closure({"2026-03"}, last_period="2027-12")
    assert "2026-08" in closure


def test_the_closure_of_nothing_is_nothing_and_it_does_not_duplicate():
    """Case 12."""
    assert balances.dirty_closure(set(), last_period="2026-12") == set()

    both = balances.dirty_closure({"2026-03", "2026-04"}, last_period="2026-12")
    assert both == {"2026-03", "2026-04", "2026-05", "2026-06"}


# --- dimension versions into periods ----------------------------------------


@pytest.fixture
def staged(tmp_path, spark):
    """A staging layer to resolve dimension versions against."""

    def build(entries, centres=(CENTRE,), accounts=(EXPENSE,), rates=(RATE,)):
        raw_dir, staging = tmp_path / "raw", tmp_path / "staging"
        raw.merge_table(GL_ENTRY, raw_dir, entries, run_id=RUN_A)
        raw.merge_table(DIM_ACCOUNT, raw_dir, list(accounts), run_id=RUN_A)
        raw.merge_table(DIM_CC, raw_dir, list(centres), run_id=RUN_A)
        raw.merge_table(DIM_FX, raw_dir, list(rates), run_id=RUN_A)
        for contract in (DIM_ACCOUNT, DIM_CC, DIM_FX):
            scd2.build(spark, contract, raw_dir, staging)
        facts.build(spark, raw_dir, staging)
        return raw_dir, staging

    return build


def test_a_dimension_version_dirties_the_periods_it_covers(spark, staged):
    """Case 13. Valid 2026-02-01 to 2026-04-30, so February, March and April - plus the
    closure each of those drags."""
    moved = {**CENTRE, "dept_code": "DEPT-FIN", "effective_date": "2026-02-01"}
    later = {**CENTRE, "dept_code": "DEPT-OPS", "effective_date": "2026-05-01"}
    raw_dir, staging = staged(
        [entry_row(f"E-{month}", f"2026-{month:02d}-04") for month in range(1, 13)],
        centres=(CENTRE, moved, later),
    )

    state = affected.Affected(periods=[], dimension_versions=[
        {"table": "dim_cost_center_src", "key": ["CC-001"], "effective_date": "2026-02-01"}
    ])
    resolved = resolve_affected.resolve(
        spark, raw_dir, staging, state, last_period="2026-12"
    )

    # Exactly those three, not merely at least them: a resolution that returned the
    # whole year would satisfy a subset assertion and would have scheduled nine periods
    # of recomputation the dimension change could not have altered.
    assert resolved == {"2026-02", "2026-03", "2026-04"}

    # And the closure the aggregate applies on top, which is where "plus their closure"
    # in the case actually happens. The year-on-year members fall in 2027 and are
    # clipped away by the reporting range, so what is left is the months each dirty
    # period reaches through the mom lag and the rolling mean.
    assert balances.dirty_closure(resolved, last_period="2026-12") == {
        "2026-02", "2026-03", "2026-04", "2026-05", "2026-06",
    }


def test_a_dimension_version_is_intersected_with_activity(spark, staged):
    """Case 14. A period carrying no entry on that cost centre is not recomputed."""
    moved = {**CENTRE, "dept_code": "DEPT-FIN", "effective_date": "2026-02-01"}
    later = {**CENTRE, "dept_code": "DEPT-OPS", "effective_date": "2026-05-01"}
    raw_dir, staging = staged(
        [entry_row("E-feb", "2026-02-04"), entry_row("E-apr", "2026-04-04")],
        centres=(CENTRE, moved, later),
    )

    version = {"table": "dim_cost_center_src", "key": ["CC-001"],
               "effective_date": "2026-02-01"}
    covered = resolve_affected.periods_for_version(
        spark, staging, version, last_period="2026-12"
    )

    # The interval covers February, March and April. Only February and April carry an
    # entry on this cost centre, so March is not scheduled by the dimension change.
    assert covered == {"2026-02", "2026-04"}

    # `resolve` reports what is owed and does not close it - the closure belongs to the
    # aggregate that defines it, and applying it in both places would widen the set
    # twice. So March is absent here and present once the closure is applied, and for a
    # different reason: February's balance changed and March's rolling mean reads
    # February. Asserted so the two mechanisms are not confused for one another.
    state = affected.Affected(periods=[], dimension_versions=[version])
    resolved = resolve_affected.resolve(
        spark, raw_dir, staging, state, last_period="2026-12"
    )
    assert resolved == {"2026-02", "2026-04"}
    assert "2026-03" in balances.dirty_closure(resolved, last_period="2026-12")


def test_the_sentinel_is_clipped_to_the_reporting_range(spark, staged):
    """Case 15. The version in force ends at 9999-12-31 - see docs/adr/0024 - and that
    is a date, not a number of periods to recompute."""
    moved = {**CENTRE, "dept_code": "DEPT-FIN", "effective_date": "2026-02-01"}
    raw_dir, staging = staged(
        [entry_row(f"E-{month}", f"2026-{month:02d}-04") for month in range(1, 13)],
        centres=(CENTRE, moved),
    )

    state = affected.Affected(periods=[], dimension_versions=[
        {"table": "dim_cost_center_src", "key": ["CC-001"], "effective_date": "2026-02-01"}
    ])
    resolved = resolve_affected.resolve(
        spark, raw_dir, staging, state, last_period="2026-12"
    )

    assert max(resolved) <= "2026-12"


def test_an_fx_version_dirties_only_the_periods_carrying_that_currency(spark, staged):
    """Case 16. A rate is a dimension - docs/adr/0029 - so a new rate dirties periods
    the same way an org change does, and only where that currency was posted."""
    rates = (
        RATE,
        {"currency": "EUR", "rate_date": "2026-01-01", "rate_to_base": "7.800000"},
        {"currency": "EUR", "rate_date": "2026-03-01", "rate_to_base": "7.900000"},
    )
    raw_dir, staging = staged(
        [entry_row("E-mar-eur", "2026-03-04", currency="EUR"),
         entry_row("E-may-cny", "2026-05-04", currency="CNY")],
        rates=rates,
    )

    version = {"table": "fx_rate", "key": ["EUR"], "effective_date": "2026-03-01"}
    covered = resolve_affected.periods_for_version(
        spark, staging, version, last_period="2026-12"
    )

    # The EUR rate published on 1 March runs to the end of the range, so its interval
    # covers May too - but May's only entry is in the base currency, so the rate change
    # could not have altered it and it is not scheduled.
    assert covered == {"2026-03"}
    assert "2026-05" not in covered


def test_a_crash_during_eviction_leaves_the_evicted_period_owed(tmp_path, monkeypatch):
    """Case 8d. The other write boundary.

    The eviction sweep rewrites the partition a key left, so that period changed. If it
    recorded only on returning, a sweep that rewrote March and then raised on April
    would lose March for good: a retry finds March already evicted and the destination
    partitions already correct, so neither the merge loop nor the sweep would record it.

    March and April each keep a survivor, so the sweep rewrites both rather than
    unlinking them, and the failure is injected on the sweep's write - the second write
    to a partition in this run, the first being the merge loop's.
    """
    source, raw_dir = tmp_path / "source", tmp_path / "raw"
    original = [entry_row("E-1", "2026-03-04"), entry_row("E-A", "2026-03-05"),
                entry_row("E-2", "2026-04-04"), entry_row("E-B", "2026-04-05")]
    full_source(source, entries=original)
    load.load_source(source, raw_dir)
    affected.clear(raw_dir)

    real = raw.write_partition
    seen: dict[str, int] = {}

    def failing(contract, path, rows, *, run_id):
        if contract["table"] == "gl_entry":
            seen[str(path)] = seen.get(str(path), 0) + 1
            if seen[str(path)] == 2 and "2026-04" in str(path):
                raise RuntimeError("interrupted during the sweep")
        return real(contract, path, rows, run_id=run_id)

    full_source(source, entries=[
        entry_row("E-1", "2026-09-04", posted_at="2026-09-04"),
        entry_row("E-A", "2026-03-05"),
        entry_row("E-2", "2026-10-04", posted_at="2026-10-04"),
        entry_row("E-B", "2026-04-05"),
    ])
    monkeypatch.setattr(raw, "write_partition", failing)
    with pytest.raises(RuntimeError):
        load.load_source(source, raw_dir, full=True)
    monkeypatch.undo()

    # March was swept before April raised, and nothing else will ever notice: the
    # destination partitions are already correct, so a retry changes nothing there.
    assert "2026-03" in affected.read(raw_dir).periods
