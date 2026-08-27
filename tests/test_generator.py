"""The synthetic ledger generator.

Every later test in this repository plants a failure with this generator and asserts
the pipeline responds to it. So the switches are tested in both directions: a switch
that is on must produce its failure mode, and a switch that is off must not. A test
that only checks the first half would pass against a generator that produced the
failure unconditionally, which would poison every downstream test with a dirty
baseline.
"""

import csv
import subprocess
import sys
import tracemalloc
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import pytest

from generator import Config, generate
from generator import schema
from generator.streams import stream_for

TABLES = ("gl_entry", "gl_adjustment", "dim_account_src", "dim_cost_center_src", "fx_rate")

# Thresholds are constants, not judgements: a vague threshold can be tripped by clean
# data or satisfied by a generator that plants nothing.
LATE_BY_DAYS = 30
GROWTH_RATIO = Decimal("1.5")
GROWTH_MONTHS = 3
OUTLIER_MULTIPLE = 20


# --- helpers ---------------------------------------------------------------

def run(tmp_path: Path, **switches) -> Path:
    """Generate into a fresh directory and return it."""
    out = tmp_path / f"out_{len(list(tmp_path.iterdir()))}"
    generate(Config(seed=42, out_dir=out, **switches))
    return out


def rows(out: Path, table: str) -> list[dict[str, str]]:
    with (out / f"{table}.csv").open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def raw_bytes(out: Path, table: str) -> bytes:
    return (out / f"{table}.csv").read_bytes()


def amount(row: dict[str, str]) -> Decimal:
    return Decimal(row["amount_dr"]) + Decimal(row["amount_cr"])


def monthly_totals(entries: list[dict[str, str]]) -> dict[str, dict[str, Decimal]]:
    totals: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
    for row in entries:
        totals[row["account_code"]][row["accounting_date"][:7]] += amount(row)
    return totals


def median(values: list[Decimal]) -> Decimal:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


# --- predicates: each is used in both directions, cases 1-7 and case 9 -----

def has_late_entries(out: Path) -> bool:
    return any(
        (schema.parse_date(row["posted_at"]) - schema.period_close(row["accounting_date"])).days
        >= LATE_BY_DAYS
        for row in rows(out, "gl_entry")
    )


def has_restatements(out: Path) -> bool:
    kinds = {row["adjustment_type"] for row in rows(out, "gl_adjustment")}
    return "restatement" in kinds


def has_cost_centre_move(out: Path) -> bool:
    seen: dict[str, set[str]] = defaultdict(set)
    for row in rows(out, "dim_cost_center_src"):
        seen[row["cc_code"]].add(row["dept_code"])
    return any(len(depts) > 1 for depts in seen.values())


def has_schema_drift(out: Path, table: str) -> bool:
    with (out / f"{table}.csv").open(encoding="utf-8", newline="") as handle:
        header = next(csv.reader(handle))
    return header != list(schema.COLUMNS[table])


def has_unbalanced_vouchers(out: Path) -> bool:
    per_doc: dict[str, tuple[Decimal, Decimal]] = defaultdict(lambda: (Decimal(0), Decimal(0)))
    for row in rows(out, "gl_entry"):
        dr, cr = per_doc[row["doc_id"]]
        per_doc[row["doc_id"]] = (dr + Decimal(row["amount_dr"]), cr + Decimal(row["amount_cr"]))
    return any(dr != cr for dr, cr in per_doc.values())


def _next_month(key: str) -> str:
    year, month = int(key[:4]), int(key[5:7])
    return f"{year + month // 12:04d}-{month % 12 + 1:02d}"


def has_growing_account(out: Path) -> bool:
    """Three consecutive month-on-month rises of at least GROWTH_RATIO.

    Consecutive means calendar-adjacent. Sorting the months that happen to be
    present would let a gap read as adjacency: a jump across a missing month is not
    the pattern this looks for.
    """
    for months in monthly_totals(rows(out, "gl_entry")).values():
        keys = sorted(months)
        run_length = 0
        for previous_key, current_key in zip(keys, keys[1:]):
            adjacent = current_key == _next_month(previous_key)
            previous, current = months[previous_key], months[current_key]
            grew = previous > 0 and current >= previous * GROWTH_RATIO
            run_length = run_length + 1 if adjacent and grew else 0
            if run_length >= GROWTH_MONTHS:
                return True
    return False


def has_amount_outliers(out: Path) -> bool:
    by_account: dict[str, list[Decimal]] = defaultdict(list)
    for row in rows(out, "gl_entry"):
        by_account[row["account_code"]].append(amount(row))
    return any(
        max(amounts) >= median(amounts) * OUTLIER_MULTIPLE
        for amounts in by_account.values()
        if median(amounts) > 0
    )


# --- Cases 1-7: one switch each, asserted in both directions ---------------

def test_late_entries_switch(tmp_path):
    assert has_late_entries(run(tmp_path, late_entries=True))
    assert not has_late_entries(run(tmp_path))


def test_restatements_switch(tmp_path):
    out = run(tmp_path, restatements=True)
    kinds = {row["adjustment_type"] for row in rows(out, "gl_adjustment")}
    assert {"correction", "restatement"} <= kinds

    known = {row["entry_id"] for row in rows(out, "gl_entry")}
    dangling = [
        row["adjusts_entry_id"]
        for row in rows(out, "gl_adjustment")
        if row["adjusts_entry_id"] not in known
    ]
    assert not dangling, f"adjustments point at entries that do not exist: {dangling[:5]}"

    assert not has_restatements(run(tmp_path))


def test_cost_centre_move_switch(tmp_path):
    out = run(tmp_path, cost_centre_move=True)
    moved = [
        row for row in rows(out, "dim_cost_center_src")
        if sum(1 for other in rows(out, "dim_cost_center_src") if other["cc_code"] == row["cc_code"]) > 1
    ]
    assert moved, "no cost centre has more than one row"
    dates = {row["effective_date"] for row in moved}
    assert len(dates) > 1, "the two rows share an effective_date"

    off = run(tmp_path)
    codes = [row["cc_code"] for row in rows(off, "dim_cost_center_src")]
    assert len(codes) == len(set(codes)), "with the switch off every cc_code should appear once"


@pytest.mark.parametrize("drift", ["add_column", "drop_column"])
@pytest.mark.parametrize("table", TABLES)
def test_schema_drift_switch(tmp_path, drift, table):
    out = run(tmp_path, schema_drift=drift, schema_drift_table=table)
    declared = list(schema.COLUMNS[table])

    with (out / f"{table}.csv").open(encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        data_rows = list(reader)

    # Exact headers, not lengths: a length check passes whatever column moved.
    if drift == "add_column":
        assert header == declared + [schema.DRIFT_COLUMN]
    else:
        dropped = schema.DRIFT_DROP_COLUMN[table]
        assert header == [column for column in declared if column != dropped]

    # A header-only change would produce malformed CSV and mask the contract failure
    # this switch exists to exercise.
    assert data_rows, "no data rows to check"
    assert all(len(row) == len(header) for row in data_rows)

    # Only the named table drifts.
    for other in TABLES:
        if other != table:
            assert not has_schema_drift(out, other), f"{other} drifted too"


def test_schema_drift_off_matches_the_declared_columns(tmp_path):
    out = run(tmp_path)
    for table in TABLES:
        assert not has_schema_drift(out, table), f"{table} header drifted with the switch off"


def test_unbalanced_vouchers_switch(tmp_path):
    assert has_unbalanced_vouchers(run(tmp_path, unbalanced_vouchers=True))
    assert not has_unbalanced_vouchers(run(tmp_path))


def test_growing_account_switch(tmp_path):
    assert has_growing_account(run(tmp_path, growing_account=True))
    assert not has_growing_account(run(tmp_path))


def test_amount_outliers_switch(tmp_path):
    assert has_amount_outliers(run(tmp_path, amount_outliers=True))
    assert not has_amount_outliers(run(tmp_path))


# --- Case 8 ----------------------------------------------------------------

def test_the_same_seed_gives_identical_bytes(tmp_path):
    first = run(tmp_path)
    second = run(tmp_path)
    for table in TABLES:
        assert raw_bytes(first, table) == raw_bytes(second, table), f"{table} differs between runs"

    other = tmp_path / "other_seed"
    generate(Config(seed=43, out_dir=other))
    assert any(raw_bytes(other, table) != raw_bytes(first, table) for table in TABLES), (
        "a different seed produced identical output, so the seed is not being used"
    )

    # The clean baseline alone would not catch a switch that is itself
    # non-deterministic, and the switches are the interesting part.
    loaded = dict(
        late_entries=True, restatements=True, cost_centre_move=True,
        unbalanced_vouchers=True, growing_account=True, amount_outliers=True,
    )
    third, fourth = run(tmp_path, **loaded), run(tmp_path, **loaded)
    for table in TABLES:
        assert raw_bytes(third, table) == raw_bytes(fourth, table), (
            f"{table} differs between runs with every switch on"
        )


# --- Case 9 ----------------------------------------------------------------

def test_the_clean_baseline_trips_none_of_the_seven_predicates(tmp_path):
    out = run(tmp_path)
    assert not has_late_entries(out)
    assert not has_restatements(out)
    assert not has_cost_centre_move(out)
    assert not any(has_schema_drift(out, table) for table in TABLES)
    assert not has_unbalanced_vouchers(out)
    assert not has_growing_account(out)
    assert not has_amount_outliers(out)


# --- Case 10 ---------------------------------------------------------------

def test_peak_memory_does_not_track_row_count(tmp_path):
    def peak_for(rows_wanted: int, name: str) -> int:
        tracemalloc.start()
        generate(Config(
            seed=42,
            out_dir=tmp_path / name,
            periods="2026-01:2026-01",     # one period, so rows_wanted is the row count
            entries_per_period=rows_wanted,
        ))
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        return peak

    small = peak_for(10_000, "small")
    large = peak_for(100_000, "large")

    assert large <= small * 2, (
        f"ten times the rows took {large / small:.1f}x the peak memory "
        f"({small} -> {large} bytes); rows are being collected, not streamed"
    )


# --- Case 11 ---------------------------------------------------------------

def test_a_switch_leaves_the_data_it_does_not_own_untouched(tmp_path):
    clean = run(tmp_path)
    dirty = run(tmp_path, unbalanced_vouchers=True)

    for table in ("dim_account_src", "dim_cost_center_src", "fx_rate"):
        assert raw_bytes(clean, table) == raw_bytes(dirty, table), (
            f"{table} changed when an unrelated switch was turned on"
        )

    # Dimensions alone are not enough: they are generated before entries, so a single
    # shared random sequence would leave them identical too. The entries outside the
    # unbalanced vouchers are what make this discriminating.
    def balanced_rows(out: Path) -> list[dict[str, str]]:
        per_doc: dict[str, tuple[Decimal, Decimal]] = defaultdict(lambda: (Decimal(0), Decimal(0)))
        entries = rows(out, "gl_entry")
        for row in entries:
            dr, cr = per_doc[row["doc_id"]]
            per_doc[row["doc_id"]] = (dr + Decimal(row["amount_dr"]), cr + Decimal(row["amount_cr"]))
        unbalanced = {doc for doc, (dr, cr) in per_doc.items() if dr != cr}
        return [row for row in entries if row["doc_id"] not in unbalanced]

    assert balanced_rows(clean) == balanced_rows(dirty), (
        "entries outside the unbalanced vouchers changed, so the random streams are shared"
    )


# --- Case 12 ---------------------------------------------------------------

def test_streams_are_derived_by_name_and_are_stable_across_processes():
    # Two streams, five draws from each. Recreating the stream for every draw would
    # compare one repeated value against itself and prove nothing.
    first, second = stream_for(42, "entries"), stream_for(42, "entries")
    assert [first.random() for _ in range(5)] == [second.random() for _ in range(5)]

    entries_stream, dimensions_stream = stream_for(42, "entries"), stream_for(42, "dimensions")
    assert [entries_stream.random() for _ in range(5)] != [
        dimensions_stream.random() for _ in range(5)
    ]

    # Python salts hash() of a string per process. A derivation built on it would look
    # correct in any single run and produce different data in the next one, which no
    # same-process test can see.
    program = (
        "from generator.streams import stream_for;"
        "s = stream_for(42, 'entries');"
        "print([s.random() for _ in range(5)])"
    )
    outputs = {
        subprocess.run(
            [sys.executable, "-c", program],
            capture_output=True, text=True, check=True,
            env={"PYTHONHASHSEED": seed, "PATH": ""},
            cwd=Path(__file__).resolve().parent.parent,
        ).stdout.strip()
        for seed in ("0", "1", "12345")
    }
    assert len(outputs) == 1, f"stream output varies with PYTHONHASHSEED: {outputs}"


# --- Case 13 ---------------------------------------------------------------

def test_the_csv_bytes_do_not_depend_on_the_platform(tmp_path):
    out = run(tmp_path)
    for table in TABLES:
        data = raw_bytes(out, table)
        assert b"\r" not in data, f"{table} contains CR; the terminator should be LF alone"
        assert not data.startswith(b"\xef\xbb\xbf"), f"{table} starts with a UTF-8 BOM"
        data.decode("utf-8")

        with (out / f"{table}.csv").open(encoding="utf-8", newline="") as handle:
            assert next(csv.reader(handle)) == list(schema.COLUMNS[table])

    numeric = {"amount_dr", "amount_cr", "rate_to_base"}
    dated = {"accounting_date", "posted_at", "effective_date", "rate_date"}
    for table in TABLES:
        for row in rows(out, table):
            for column, value in row.items():
                if column in numeric:
                    assert value == str(Decimal(value).quantize(Decimal("0.01"))), (
                        f"{table}.{column}={value!r} is not at a fixed two decimal places"
                    )
                if column in dated:
                    assert schema.format_date(schema.parse_date(value)) == value, (
                        f"{table}.{column}={value!r} is not YYYY-MM-DD"
                    )


# --- Added at stage 8 -------------------------------------------------------

ALL_SWITCHES = {
    "late_entries": "gl_entry",
    "restatements": "gl_adjustment",
    "cost_centre_move": "dim_cost_center_src",
    "unbalanced_vouchers": "gl_entry",
    "growing_account": "gl_entry",
    "amount_outliers": "gl_entry",
    "long_tail_anomaly": "gl_entry",
}


def test_every_reference_resolves(tmp_path):
    """Accounts, cost centres and currencies used by entries must exist.

    Nothing checked this, and the growth anomaly was posting to two account codes
    that dim_account_src never emitted. Every anomaly predicate passed while the
    generator produced orphan rows.
    """
    out = run(tmp_path, **{switch: True for switch in ALL_SWITCHES})

    accounts = {row["account_code"] for row in rows(out, "dim_account_src")}
    centres = {row["cc_code"] for row in rows(out, "dim_cost_center_src")}
    currencies = {row["currency"] for row in rows(out, "fx_rate")}

    for table in ("gl_entry", "gl_adjustment"):
        for row in rows(out, table):
            assert row["account_code"] in accounts, (
                f"{table}.{row['entry_id']} posts to account {row['account_code']}, "
                f"which is not in dim_account_src"
            )
            assert row["cost_center_code"] in centres, (
                f"{table}.{row['entry_id']} posts to cost centre {row['cost_center_code']}, "
                f"which is not in dim_cost_center_src"
            )
            assert row["currency"] in currencies, (
                f"{table}.{row['entry_id']} uses {row['currency']}, which has no fx rate"
            )


@pytest.mark.parametrize("switch,owned", sorted(ALL_SWITCHES.items()))
def test_a_switch_touches_only_the_table_it_owns(tmp_path, switch, owned):
    """Case 11 covered unbalanced_vouchers only. Every switch shares tables with the
    others, so every switch needs the same guarantee."""
    clean = run(tmp_path)
    dirty = run(tmp_path, **{switch: True})

    for table in TABLES:
        if table == owned:
            continue
        assert raw_bytes(clean, table) == raw_bytes(dirty, table), (
            f"{switch} changed {table}, which it does not own"
        )

    # Skipping the owned table entirely proved nothing about the four switches that
    # all own gl_entry. Every planted row carries an `X-` prefix, so the ordinary
    # entries must be untouched whichever switch is on.
    def ordinary_entries(out: Path) -> list[dict[str, str]]:
        return [row for row in rows(out, "gl_entry") if row["entry_id"].startswith("E-")]

    assert ordinary_entries(clean) == ordinary_entries(dirty), (
        f"{switch} disturbed the ordinary entries, so it shares a random stream"
    )


def test_the_command_line_writes_the_five_tables(tmp_path):
    """The CLI is how step four drives scale, so its wiring is user-facing."""
    from generator.__main__ import main

    out = tmp_path / "cli"
    main([
        "--seed", "7",
        "--periods", "2026-01:2026-02",
        "--entries-per-period", "50",
        "--out", str(out),
        "--enable-unbalanced-vouchers",
    ])

    assert sorted(path.name for path in out.glob("*.csv")) == sorted(
        f"{table}.csv" for table in TABLES
    )
    assert has_unbalanced_vouchers(out), "--enable-unbalanced-vouchers did not reach the config"
    assert not has_late_entries(out), "a switch that was not asked for was turned on"


@pytest.mark.parametrize("periods", ["2026-01", "2026-12:2026-01", "nonsense"])
def test_a_bad_period_range_fails_loudly(periods):
    """These used to produce an empty month list and surface much later as a
    ZeroDivisionError somewhere unrelated."""
    with pytest.raises(ValueError, match="periods"):
        generate(Config(seed=42, out_dir=Path("unused"), periods=periods))


def test_growing_account_refuses_a_range_too_short_to_show_growth(tmp_path):
    """Three month-on-month rises need four monthly totals. Silently doing nothing
    would leave the switch on and the anomaly absent."""
    with pytest.raises(ValueError, match="growing_account needs at least"):
        generate(Config(
            seed=42, out_dir=tmp_path / "short",
            periods="2026-01:2026-02", growing_account=True,
        ))
