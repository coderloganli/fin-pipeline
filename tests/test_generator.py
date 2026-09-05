"""The synthetic ledger generator.

Every later test in this repository plants a failure with this generator and asserts
the pipeline responds to it. So the switches are tested in both directions: a switch
that is on must produce its failure mode, and a switch that is off must not. A test
that only checks the first half would pass against a generator that produced the
failure unconditionally, which would poison every downstream test with a dirty
baseline.
"""

import ast
import csv
import random
from calendar import monthrange
import subprocess
import sys
import tracemalloc
from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from generator import Config, generate
from generator import dimensions, schema
from generator.streams import stream_for

TABLES = ("gl_entry", "gl_adjustment", "dim_account_src", "dim_cost_center_src",
          "dim_vendor", "fx_rate")

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

    # Amounts and rates are different quantities with different widths. Asserting one
    # rule over both is what let a rate sit at two decimal places, which is a 0.06%
    # error on every conversion. See docs/adr/0013-a-rate-is-not-an-amount.md.
    amounts = {"amount_dr", "amount_cr"}
    rates = {"rate_to_base"}
    dated = {"accounting_date", "posted_at", "effective_date", "rate_date"}
    for table in TABLES:
        for row in rows(out, table):
            for column, value in row.items():
                if column in amounts:
                    assert value == str(Decimal(value).quantize(Decimal("0.01"))), (
                        f"{table}.{column}={value!r} is not at a fixed two decimal places"
                    )
                if column in rates:
                    assert value == str(Decimal(value).quantize(schema.RATE_PLACES)), (
                        f"{table}.{column}={value!r} is not at a fixed six decimal places"
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


def test_the_command_line_writes_the_six_tables(tmp_path):
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


# --- Exchange-rate precision ------------------------------------------------
#
# A rate is not a currency amount. Two decimal places on a rate near 7.87 is a
# quantisation error of 0.06% on every foreign-currency conversion the platform will
# compute, and a rerun reproduces it faithfully rather than exposing it. See
# docs/adr/0013-a-rate-is-not-an-amount.md.

NON_BASE_CURRENCIES = ("EUR", "USD", "GBP")

# The exact reachable bounds, not a rounded band. RATE_MAX_MICROS is exclusive and the
# drift is applied with floor division, so the ceiling is 8_999_999 * 1_020_000 //
# 1_000_000 = 9_179_998 rather than 9.18.
RATE_FLOOR = Decimal("4.900000")
RATE_CEILING = Decimal("9.179998")


def rates_by_currency(out: Path) -> dict[str, list[str]]:
    found: dict[str, list[str]] = defaultdict(list)
    for row in rows(out, "fx_rate"):
        found[row["currency"]].append(row["rate_to_base"])
    return found


def test_every_rate_is_written_to_six_decimal_places(tmp_path):
    """Case 1. Asserted on the text in the file: the width is the point, and a numeric
    comparison would be satisfied by Decimal("7.87")."""
    for value in (row["rate_to_base"] for row in rows(run(tmp_path), "fx_rate")):
        fraction = value.partition(".")[2]
        assert len(fraction) == 6, f"rate_to_base={value!r} is not at six decimal places"


def test_the_base_currency_is_written_as_one_at_full_width(tmp_path):
    """Case 2. One shape for the column, so no reader special-cases the identity."""
    written = rates_by_currency(run(tmp_path))["CNY"]
    assert written, "no CNY rows were generated"
    assert set(written) == {"1.000000"}, f"CNY rates are {sorted(set(written))}"


@pytest.mark.parametrize("currency", NON_BASE_CURRENCIES)
def test_the_extra_digits_carry_information(tmp_path, currency):
    """Case 3. The one that makes this a fix rather than a wider column.

    A format_rate that padded 7.87 to 7.870000 satisfies cases 1 and 2 while leaving
    the 0.06% error exactly where it was. Asserted per currency: pooled over the
    column, one currency with real six-place variation would carry two others still
    padded from two places, and two thirds of the defect would pass.
    """
    written = rates_by_currency(run(tmp_path))[currency]
    assert written, f"no {currency} rows were generated"

    distinct = {Decimal(value) for value in written}
    rounded = {value.quantize(Decimal("0.01")) for value in distinct}
    assert len(distinct) > len(rounded), (
        f"{currency} has {len(distinct)} distinct rates but only {len(rounded)} "
        f"distinct values at two decimal places; the extra digits carry nothing"
    )


def test_every_rate_lands_in_the_reachable_band(tmp_path):
    """Case 4. Exact bounds, so an off-by-one at the top of the centre range fails
    here rather than looking like a plausible number three layers downstream. Also
    the floor the contract declares: fx_rate.yaml says min 0.000001."""
    contract_floor = Decimal("0.000001")
    for currency, written in rates_by_currency(run(tmp_path)).items():
        for value in written:
            rate = Decimal(value)
            assert rate >= contract_floor, f"{currency} rate {value} is below the contract minimum"
            if currency == "CNY":
                continue
            assert RATE_FLOOR <= rate <= RATE_CEILING, (
                f"{currency} rate {value} is outside the reachable band "
                f"[{RATE_FLOOR}, {RATE_CEILING}]"
            )


@pytest.mark.parametrize("currency", NON_BASE_CURRENCIES)
def test_a_rate_moves_from_day_to_day(tmp_path, currency):
    """Case 5. Guards a refactor that gets the centre right and drops the daily term."""
    written = rates_by_currency(run(tmp_path))[currency]
    assert len(set(written)) > 1, f"{currency} held one rate for every day: {written[0]}"


class NoFloats(random.Random):
    """A stream that refuses to produce a float.

    randrange reaches getrandbits rather than random(), so an integer draw is
    unaffected. That is CPython's implementation rather than a documented guarantee;
    the project pins CPython 3.13 (docs/adr/0002-python-version.md), and if it stopped
    holding this fails loudly rather than passing quietly.
    """

    FLOAT_METHODS = (
        "random", "uniform", "gauss", "normalvariate", "lognormvariate", "triangular",
        "betavariate", "expovariate", "paretovariate", "weibullvariate",
        "vonmisesvariate",
    )


def _refuse(name):
    def method(self, *args, **kwargs):
        raise AssertionError(f"the rate path drew a float through {name}()")
    return method


for _name in NoFloats.FLOAT_METHODS:
    setattr(NoFloats, _name, _refuse(_name))


def test_the_rate_path_draws_no_float(monkeypatch):
    """Case 6. Both a float draw and an integer draw produce six-place strings, so no
    output distinguishes them. Refusing the whole float half of the Random API is a
    property of the behaviour rather than of the source text - unlike a ban on the
    word `uniform`, which `rng.random()` walks straight past.
    """
    monkeypatch.setattr(
        dimensions, "stream_for", lambda seed, name: NoFloats(hash((seed, name)) & 0xFFFF)
    )
    produced = dimensions.fx_rates(42, [date(2026, 1, 1)])
    assert produced, "fx_rates produced no rows"
    assert all(row["rate_to_base"] for row in produced)


def _function_source(module, name: str) -> ast.AST:
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{module.__name__} defines no {name}")


@pytest.mark.parametrize("module,function", [
    (dimensions, "fx_rates"),
    (dimensions, "_rate"),
    (schema, "format_rate"),
])
def test_no_float_reaches_the_rate_arithmetic(module, function):
    """Case 6b. What case 6 cannot see: a stream can hand back honest integers and the
    code can still multiply one of them by 1.02."""
    node = _function_source(module, function)

    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, float):
            raise AssertionError(
                f"{module.__name__}.{function} contains the float literal {child.value!r}"
            )
        # Both spellings: `float(x)` and `builtins.float(x)`. The second is only
        # reachable on purpose, but a check that names one and not the other invites
        # exactly that.
        if isinstance(child, ast.Call):
            called = getattr(child.func, "id", None) or getattr(child.func, "attr", None)
            assert called != "float", (
                f"{module.__name__}.{function} calls float()"
            )


def test_a_rate_has_its_own_formatter_and_the_generator_uses_it():
    """Case 7. The last assertion is the one that matters: an inline
    .quantize(schema.RATE_PLACES) in dimensions.py satisfies every behavioural case
    while bypassing the formatter this change exists to introduce."""
    assert schema.format_rate is not schema.format_amount
    assert schema.format_rate(Decimal(1)) == "1.000000"
    assert schema.format_amount(Decimal(1)) == "1.00"

    called = {
        child.func.attr
        for child in ast.walk(_function_source(dimensions, "fx_rates"))
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute)
    }
    assert "format_rate" in called, "fx_rates does not call format_rate"


# --- Added at stage 8 -------------------------------------------------------


class ScriptedStream(random.Random):
    """Records every randrange call, and answers from a script while it lasts.

    The generated sample cannot reach the edges of the draw range on its own, and the
    draw *pattern* leaves no trace in the output at all - so both are asserted by
    driving fx_rates with a stream that is watched and, where it matters, dictated.
    """

    def __init__(self, values=None):
        super().__init__(0)
        self.calls: list[tuple] = []
        self._scripted = list(values or [])

    def randrange(self, *args, **kwargs):
        self.calls.append(args)
        if self._scripted:
            return self._scripted.pop(0)
        return super().randrange(*args, **kwargs)


DRIVEN_MONTH = date(2026, 1, 1)
DRIVEN_DAYS = monthrange(DRIVEN_MONTH.year, DRIVEN_MONTH.month)[1]


def drive(monkeypatch, values=None) -> tuple[ScriptedStream, list[dict]]:
    stream = ScriptedStream(values)
    monkeypatch.setattr(dimensions, "stream_for", lambda seed, name: stream)
    return stream, dimensions.fx_rates(42, [DRIVEN_MONTH])


@pytest.mark.parametrize("centre,drift,expected", [
    # The top of the range: RATE_MAX_MICROS is exclusive and the division floors, so
    # the highest reachable rate is 8_999_999 * 1_020_000 // 1_000_000 = 9_179_998.
    # An off-by-one that made the centre bound inclusive would produce 9.180000 here.
    (dimensions.RATE_MAX_MICROS - 1, dimensions.DRIFT_PPM, "9.179998"),
    # The bottom, which is exact.
    (dimensions.RATE_MIN_MICROS, -dimensions.DRIFT_PPM, "4.900000"),
])
def test_the_edges_of_the_draw_range_land_where_the_band_says(
    monkeypatch, centre, drift, expected
):
    """Case 9. Case 4 asserts the band over a seed-42 sample, which cannot reach its
    own edges: an inclusive upper bound would pass there unless the sample happened to
    draw it. This dictates the boundary draw instead of waiting for it.

    The script feeds CNY's discarded drift for every day of the month - the base
    currency takes one per day, not one per month - and then EUR's centre and its
    first drift.
    """
    _, produced = drive(monkeypatch, [0] * DRIVEN_DAYS + [centre, drift])
    eur = [row for row in produced if row["currency"] == "EUR"]
    assert eur[0]["rate_to_base"] == expected


def test_the_draws_taken_per_currency_are_exactly_the_documented_ones(monkeypatch):
    """Case 10. The stream cursor is a contract, and it is invisible in the output.

    CNY takes no centre draw and a discarded drift draw every day. Dropping either
    would shift EUR, USD and GBP - silently, because every rate would still be a
    well-formed six-place number inside the band. See
    docs/adr/0013-a-rate-is-not-an-amount.md.
    """
    stream, produced = drive(monkeypatch)

    centre_bounds = (dimensions.RATE_MIN_MICROS, dimensions.RATE_MAX_MICROS)
    drift_bounds = (-dimensions.DRIFT_PPM, dimensions.DRIFT_PPM + 1)

    days = len({row["rate_date"] for row in produced})
    non_base = [c for c in dimensions.CURRENCIES if c != dimensions.BASE_CURRENCY]

    assert stream.calls.count(centre_bounds) == len(non_base), (
        f"expected one centre draw per non-base currency, got {stream.calls}"
    )
    assert stream.calls.count(drift_bounds) == len(dimensions.CURRENCIES) * days, (
        "expected a drift draw for every currency on every day, including the base "
        f"currency whose value is discarded; got {stream.calls}"
    )
    assert len(stream.calls) == len(non_base) + len(dimensions.CURRENCIES) * days

    # And the base currency's first draw is a drift, not a centre: it takes no centre.
    assert stream.calls[0] == drift_bounds


def test_the_rate_written_is_the_one_the_formatter_returned(monkeypatch):
    """Case 11. Case 7 asserts that a call named format_rate appears somewhere in
    fx_rates, which a dead call alongside an inline quantize would satisfy. This
    asserts the value in the row came through the formatter."""
    monkeypatch.setattr(schema, "format_rate", lambda value: "through-the-formatter")
    _, produced = drive(monkeypatch)

    assert produced
    assert {row["rate_to_base"] for row in produced} == {"through-the-formatter"}


# --- a chart of accounts, cost centres and vendors that read like a ledger ---
#
# Cases 1-8, 9-11, 12-17a, 20a and 23 of task.md. The generator's structure was
# always right; what these pin is that its content means something. See
# docs/adr/0021 (the chart follows the standard) and 0022 (a vendor belongs to the
# voucher, and is drawn from its own stream).

from generator import entries as entry_module            # noqa: E402
from generator import streams as stream_module           # noqa: E402

FIRST_LEVEL_DIGITS = 4
DETAIL_DIGITS = 6


def by_voucher(out: Path, prefix: str = "D-") -> dict[str, list[dict[str, str]]]:
    """Ordinary vouchers, keyed by document. Planted rows carry their own prefixes."""
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows(out, "gl_entry"):
        if row["doc_id"].startswith(prefix):
            grouped[row["doc_id"]].append(row)
    return dict(grouped)


def debit_of(voucher: list[dict[str, str]]) -> dict[str, str]:
    return next(row for row in voucher if Decimal(row["amount_dr"]) > 0)


def credit_of(voucher: list[dict[str, str]]) -> dict[str, str]:
    return next(row for row in voucher if Decimal(row["amount_cr"]) > 0)


# --- the chart --------------------------------------------------------------

@pytest.mark.parametrize("code, expected_type", [
    ("1001", "asset"),
    ("2202", "liability"),
    ("4001", "equity"),
    ("6001", "revenue"),
    ("6601", "expense"),
])
def test_an_accounts_code_name_and_type_agree(tmp_path, code, expected_type):
    """Case 1. The old chart typed 6100 as an asset, which is not a chart anybody
    could read: under the standard the 6xxx range is profit and loss."""
    declared = {row["account_code"]: row for row in rows(run(tmp_path), "dim_account_src")}
    assert code in declared, f"{code} is not in the chart"
    assert declared[code]["account_type"] == expected_type
    assert declared[code]["name"], f"{code} has no name"
    assert not declared[code]["name"].startswith("account "), (
        f"{code} is still named by its own code"
    )


def test_a_detail_account_hangs_off_a_first_level_one(tmp_path):
    """Case 2. Exactly two levels: a row with a parent names a row without one."""
    declared = {row["account_code"]: row for row in rows(run(tmp_path), "dim_account_src")}
    details = [row for row in declared.values() if row["parent_code"]]
    assert details, "no detail accounts at all"
    for row in details:
        parent = row["parent_code"]
        assert parent in declared, f"{row['account_code']} names a parent that is absent"
        assert not declared[parent]["parent_code"], (
            f"{row['account_code']} hangs off {parent}, which itself has a parent"
        )


def test_a_first_level_account_has_no_parent(tmp_path):
    """Case 3. The code says which level it is, and the two agree."""
    for row in rows(run(tmp_path), "dim_account_src"):
        if row["parent_code"]:
            assert len(row["account_code"]) == DETAIL_DIGITS
            assert row["account_code"].startswith(row["parent_code"])
        else:
            assert len(row["account_code"]) == FIRST_LEVEL_DIGITS


def test_account_codes_are_unique(tmp_path):
    """Case 4."""
    codes = [row["account_code"] for row in rows(run(tmp_path), "dim_account_src")]
    assert len(codes) == len(set(codes))


def test_every_account_type_appears_more_than_once(tmp_path):
    """Case 5. A chart with one equity account is a chart nobody modelled."""
    seen = defaultdict(int)
    for row in rows(run(tmp_path), "dim_account_src"):
        seen[row["account_type"]] += 1
    assert set(seen) == set(dimensions.ACCOUNT_TYPES)
    assert all(count >= 2 for count in seen.values()), dict(seen)


@pytest.mark.parametrize("switches", [
    {},
    {"long_tail_anomaly": True},
    {"growing_account": True},
])
def test_the_reserved_anomaly_accounts_are_always_declared(tmp_path, switches):
    """Case 6. The property docs/adr/0007 gave them, carried across the renumbering:
    they are emitted whether or not their switch is on, so turning one on does not
    change dim_account_src."""
    declared = {row["account_code"] for row in rows(run(tmp_path, **switches), "dim_account_src")}
    for account in dimensions.RESERVED_ACCOUNTS:
        assert account in declared, f"{account} is missing with {switches}"


# --- cost centres -----------------------------------------------------------

def test_cost_centres_have_real_names_and_real_departments(tmp_path):
    """Case 7."""
    for row in rows(run(tmp_path), "dim_cost_center_src"):
        assert row["name"] != f"cost centre {row['cc_code']}"
        assert row["dept_code"] in dimensions.DEPARTMENTS, row["dept_code"]


def test_the_cost_centre_move_rotates_within_the_departments(tmp_path):
    """Case 8. The move used to be computed by reading the last character of the
    department code as a number, which a code like DEPT-SALES has no answer for."""
    seen: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows(run(tmp_path, cost_centre_move=True), "dim_cost_center_src"):
        seen[row["cc_code"]].append(row)

    moved = [history for history in seen.values() if len(history) > 1]
    assert len(moved) == 1, "expected exactly one cost centre to move"

    before, after = sorted(moved[0], key=lambda row: row["effective_date"])
    assert before["dept_code"] != after["dept_code"]
    assert after["dept_code"] in dimensions.DEPARTMENTS


# --- the vendor dimension ---------------------------------------------------

def test_dim_vendor_has_one_row_per_vendor(tmp_path):
    """Case 9."""
    vendors = rows(run(tmp_path), "dim_vendor")
    assert len(vendors) == dimensions.VENDOR_COUNT
    codes = [row["vendor_code"] for row in vendors]
    assert len(codes) == len(set(codes))
    for row in vendors:
        assert row["name"]
        assert row["category"] in dimensions.VENDOR_CATEGORIES, row["category"]


def test_the_vendor_categories_are_shaped_for_the_two_anomaly_forms(tmp_path):
    """Case 10. The distribution is a design input, not an accident: a long tail needs
    somewhere to spread and a concentrated anomaly needs somewhere to concentrate.
    Without the asymmetry the two shapes look identical under breakdown_by_vendor,
    which is the comparison the third step is built on. See docs/adr/0022."""
    counts = defaultdict(int)
    for row in rows(run(tmp_path), "dim_vendor"):
        counts[row["category"]] += 1

    assert counts[dimensions.OFFICE] >= 25, dict(counts)
    assert counts[dimensions.MARKETING] <= 2, dict(counts)


def test_dim_vendor_is_written_with_the_declared_header(tmp_path):
    """Case 11."""
    with (run(tmp_path) / "dim_vendor.csv").open(encoding="utf-8", newline="") as handle:
        assert next(csv.reader(handle)) == list(schema.COLUMNS["dim_vendor"])


# --- the vendor on an entry -------------------------------------------------

def test_a_vendor_voucher_debits_an_expense_and_credits_a_payable(tmp_path):
    """Case 12. Asserting only that both lines share a vendor would pass a generator
    that credited anything at all; the shape is what makes the row an invoice."""
    out = run(tmp_path)
    checked = 0
    for doc_id, voucher in by_voucher(out).items():
        if not voucher[0]["vendor_code"]:
            continue
        assert len(voucher) == 2, doc_id
        debit_account = debit_of(voucher)["account_code"]
        assert debit_account in dimensions.EXPENSE_DETAIL
        category = dimensions.CATEGORY_OF_ACCOUNT[debit_account]
        # The payable the invoice is owed into, not merely some payable: a travel
        # expense owed to a materials supplier would pass a membership check.
        assert credit_of(voucher)["account_code"] == dimensions.PAYABLE_FOR_CATEGORY[category]
        assert voucher[0]["vendor_code"] == voucher[1]["vendor_code"]
        checked += 1
    assert checked, "no voucher carried a vendor"


def test_an_income_voucher_carries_no_vendor(tmp_path):
    """Case 13. Revenue is earned from customers, not paid to suppliers."""
    out = run(tmp_path)
    checked = 0
    for doc_id, voucher in by_voucher(out).items():
        if voucher[0]["vendor_code"]:
            continue
        assert len(voucher) == 2, doc_id
        assert debit_of(voucher)["account_code"] == dimensions.RECEIVABLE
        assert credit_of(voucher)["account_code"] in dimensions.REVENUE_DETAIL
        assert voucher[1]["vendor_code"] == ""
        checked += 1
    assert checked, "no voucher was free of a vendor"


def test_both_voucher_shapes_are_generated(tmp_path):
    """Case 13a. Without this, the two cases above can both pass against a generator
    that only ever produces one shape."""
    vouchers = by_voucher(run(tmp_path))
    with_vendor = [v for v in vouchers.values() if v[0]["vendor_code"]]
    without = [v for v in vouchers.values() if not v[0]["vendor_code"]]
    assert with_vendor and without, (len(with_vendor), len(without))


def test_every_vendor_on_an_entry_exists_in_the_dimension(tmp_path):
    """Case 14. Referential integrity, asserted at the source rather than waited for
    in a dbt test that does not exist yet."""
    out = run(tmp_path, late_entries=True, restatements=True, long_tail_anomaly=True,
              growing_account=True, amount_outliers=True)
    declared = {row["vendor_code"] for row in rows(out, "dim_vendor")}
    for table in ("gl_entry", "gl_adjustment"):
        for row in rows(out, table):
            if row["vendor_code"]:
                assert row["vendor_code"] in declared, (
                    f"{table}.{row['entry_id']} names vendor {row['vendor_code']}, "
                    f"which is not in dim_vendor"
                )


def test_a_description_is_never_empty_and_the_voucher_shares_it(tmp_path):
    """Case 15."""
    for voucher in by_voucher(run(tmp_path)).values():
        assert all(row["description"] for row in voucher)
        assert len({row["description"] for row in voucher}) == 1


def test_a_description_comes_from_its_accounts_category(tmp_path):
    """Case 16. Phrase sets rather than free text, and the sets are disjoint - so a
    generator that stamped any non-empty string on every row would fail here."""
    out = run(tmp_path)
    checked = 0
    for voucher in by_voucher(out).values():
        account = debit_of(voucher)["account_code"]
        category = dimensions.CATEGORY_OF_ACCOUNT.get(account)
        if category is None:
            continue
        assert voucher[0]["description"] in dimensions.PHRASES[category], (
            f"{account} ({category}) got {voucher[0]['description']!r}"
        )
        checked += 1
    assert checked, "no voucher debited an account with a category"


def test_descriptions_and_vendors_are_deterministic(tmp_path):
    """Case 17."""
    first = {row["entry_id"]: (row["vendor_code"], row["description"])
             for row in rows(run(tmp_path), "gl_entry")}
    second = {row["entry_id"]: (row["vendor_code"], row["description"])
              for row in rows(run(tmp_path), "gl_entry")}
    assert first == second


def test_an_adjustment_carries_a_vendor_when_its_account_does(tmp_path):
    """Case 17a. An adjustment is a standalone row rather than a voucher, so the
    per-voucher rule cannot apply to it; the rule is per row instead."""
    out = run(tmp_path, restatements=True)
    vendor_bearing = set(dimensions.EXPENSE_DETAIL) | set(dimensions.PAYABLE_DETAIL)
    for row in rows(out, "gl_adjustment"):
        assert row["description"], row["entry_id"]
        if row["account_code"] in vendor_bearing:
            assert row["vendor_code"], f"{row['entry_id']} posts to an expense with no vendor"
        else:
            assert row["vendor_code"] == "", f"{row['entry_id']} carries a vendor it should not"


def test_ordinary_entries_never_post_to_a_reserved_account(tmp_path):
    """Case 20a. docs/adr/0021 keeps the reserved accounts reserved so that a planted
    increase never leaks into an ordinary account's monthly totals. Renaming them does
    not change that argument, and the existing tests only check they exist."""
    out = run(tmp_path, long_tail_anomaly=True, growing_account=True, amount_outliers=True)
    for row in rows(out, "gl_entry"):
        if row["entry_id"].startswith("E-"):
            assert row["account_code"] not in dimensions.RESERVED_ACCOUNTS, (
                f"ordinary entry {row['entry_id']} posts to reserved {row['account_code']}"
            )


def test_the_vendor_stream_does_not_move_the_entries(tmp_path, monkeypatch):
    """Case 23. The whole reason docs/adr/0005 derives streams by name: a new concern
    must not shift the numbers belonging to the old ones. Only the vendors stream is
    replaced - changing Config.seed would move every stream and prove nothing."""
    declared = list(schema.COLUMNS["gl_entry"])
    business = [name for name in declared if name not in ("vendor_code", "description")]

    before = [tuple(row[name] for name in business) for row in rows(run(tmp_path), "gl_entry")]

    real = stream_module.stream_for

    def a_different_vendor_stream(seed, name):
        return real(seed + 1 if name == stream_module.VENDORS else seed, name)

    monkeypatch.setattr(dimensions, "stream_for", a_different_vendor_stream)
    monkeypatch.setattr(entry_module, "stream_for", a_different_vendor_stream)

    after = [tuple(row[name] for name in business) for row in rows(run(tmp_path), "gl_entry")]

    assert before == after


# --- what the vendor has to agree with --------------------------------------
#
# Added at stage 8, after the code review found that a voucher could carry a supplier
# of the wrong kind, or carry one inconsistently on a planted voucher, without any of
# the cases above noticing.

def all_vouchers(out: Path) -> dict[str, list[dict[str, str]]]:
    """Every voucher in gl_entry, whatever planted it."""
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows(out, "gl_entry"):
        grouped[row["doc_id"]].append(row)
    return dict(grouped)


def test_a_vendor_supplies_what_its_account_buys(tmp_path):
    """The supplier's own category has to be the account's category. Without this a
    travel expense could be owed to an office-supplies vendor and still satisfy every
    other test: the vendor exists, it is in dim_vendor, and the wording came from the
    right phrase set."""
    out = run(tmp_path, long_tail_anomaly=True, growing_account=True, amount_outliers=True)
    category_of_vendor = {row["vendor_code"]: row["category"] for row in rows(out, "dim_vendor")}

    checked = 0
    for voucher in all_vouchers(out).values():
        vendor = voucher[0]["vendor_code"]
        if not vendor:
            continue
        account = debit_of(voucher)["account_code"]
        assert category_of_vendor[vendor] == dimensions.CATEGORY_OF_ACCOUNT[account], (
            f"{account} is invoiced by {vendor}, a "
            f"{category_of_vendor[vendor]} supplier"
        )
        checked += 1
    assert checked


def test_every_voucher_carries_its_vendor_on_both_lines(tmp_path):
    """Not just the ordinary ones. The planted vouchers - late, unbalanced, growth,
    outlier - and the long tail all go through the same builder, and an aggregate taken
    from the credit side has to reach the same supplier as one taken from the debit
    side."""
    out = run(tmp_path, late_entries=True, unbalanced_vouchers=True,
              growing_account=True, amount_outliers=True, long_tail_anomaly=True)
    prefixes = set()
    for doc_id, voucher in all_vouchers(out).items():
        assert len({row["vendor_code"] for row in voucher}) == 1, doc_id
        assert len({row["description"] for row in voucher}) == 1, doc_id
        assert all(row["description"] for row in voucher), doc_id
        prefixes.add(doc_id.split("-")[0])

    # The planted paths were reached at all, rather than the loop having nothing to do.
    assert {"D", "X", "L"} <= prefixes, prefixes


def test_an_adjustment_posts_to_an_expense_and_carries_its_vendor(tmp_path):
    """Case 17a, narrowed to what the generator does. Adjustments correct expenses, so
    every one of them has a supplier; asserting the empty branch as well would be
    asserting against a path nothing produces."""
    out = run(tmp_path, restatements=True)
    category_of_vendor = {row["vendor_code"]: row["category"] for row in rows(out, "dim_vendor")}

    rows_seen = rows(out, "gl_adjustment")
    assert rows_seen
    for row in rows_seen:
        assert row["account_code"] in dimensions.EXPENSE_DETAIL, row["entry_id"]
        assert row["vendor_code"], row["entry_id"]
        assert row["description"], row["entry_id"]
        assert (category_of_vendor[row["vendor_code"]]
                == dimensions.CATEGORY_OF_ACCOUNT[row["account_code"]])


def test_the_vendors_stream_is_what_names_the_suppliers(tmp_path, monkeypatch):
    """The other half of the isolation test. That one shows the entries do not move
    when the vendors stream changes; this one shows the stream is reaching the vendors
    at all, so a generator that ignored it entirely could not pass both."""
    before = {row["vendor_code"]: row["name"] for row in rows(run(tmp_path), "dim_vendor")}

    real = stream_module.stream_for

    def a_different_vendor_stream(seed, name):
        return real(seed + 1 if name == stream_module.VENDORS else seed, name)

    monkeypatch.setattr(dimensions, "stream_for", a_different_vendor_stream)
    after = {row["vendor_code"]: row["name"] for row in rows(run(tmp_path), "dim_vendor")}

    assert set(before) == set(after)
    assert before != after
