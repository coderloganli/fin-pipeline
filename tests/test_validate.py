"""The contract validator: the first of the three quality gates.

A contract says what ingest expects a source table to look like. This module asserts
what happens when reality disagrees with it — an added column warns and the run
continues, and everything else fails the run.

The two halves are separated on purpose. Cases 1-11 drive the validator through the
generator, which is how the gate will actually be used; cases 12-29 write CSV by hand,
because the generator has no switch for a swapped header or an unparseable amount and
should not grow one. See docs/adr/0009-what-counts-as-a-compatible-schema-change.md.
"""

import ast
import csv
import importlib
import logging
import subprocess
import sys
import tracemalloc
from pathlib import Path

import pytest

from generator import Config, generate, schema
from ingest import contracts
from ingest import validate as V

REPO_ROOT = Path(__file__).resolve().parent.parent

TABLES = tuple(schema.COLUMNS)

# Everything except schema_drift, whose whole purpose is to add or remove a column.
BUSINESS_SWITCHES = (
    "late_entries",
    "restatements",
    "cost_centre_move",
    "unbalanced_vouchers",
    "growing_account",
    "amount_outliers",
    "long_tail_anomaly",
)


# --- helpers ---------------------------------------------------------------

def generated(tmp_path: Path, **switches) -> Path:
    out = tmp_path / f"data_{len(list(tmp_path.iterdir()))}"
    generate(Config(seed=42, out_dir=out, **switches))
    return out


def write_csv(directory: Path, table: str, header, rows) -> Path:
    """Write one table by hand. LF endings, matching the generator's writer."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{table}.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(list(header))
        for row in rows:
            writer.writerow([row[name] for name in header])
    return path


GL_ENTRY = {
    "entry_id": "E1",
    "version": "1",
    "accounting_date": "2026-01-31",
    "posted_at": "2026-02-03",
    "account_code": "6100",
    "cost_center_code": "CC01",
    "currency": "CNY",
    "amount_dr": "100.00",
    "amount_cr": "0.00",
    "doc_id": "D1",
}

ACCOUNT = {
    "account_code": "6100",
    "name": "Office expense",
    "parent_code": "6000",
    "account_type": "expense",
    "effective_date": "2026-01-01",
}

FX_RATE = {"currency": "USD", "rate_date": "2026-01-31", "rate_to_base": "7.1234"}


def entry(**overrides) -> dict:
    return {**GL_ENTRY, **overrides}


def one_table(tmp_path: Path, table: str, rows, header=None) -> Path:
    """A source directory holding exactly one hand-written table."""
    directory = tmp_path / f"src_{len(list(tmp_path.iterdir()))}"
    write_csv(directory, table, header or schema.COLUMNS[table], rows)
    return directory


def report_for(tmp_path: Path, table: str, rows, header=None, **kwargs):
    directory = one_table(tmp_path, table, rows, header)
    return V.validate_source(directory, tables=[table], **kwargs)


def kinds(report) -> list[str]:
    return [finding.kind for finding in report.findings]


def table_report(report, table: str):
    for entry_ in report.tables:
        if entry_.table == table:
            return entry_
    raise AssertionError(f"the report covers {[t.table for t in report.tables]}, not {table!r}")


# --- Cases 1-2: the ticket's own acceptance --------------------------------

def test_an_added_column_passes_with_a_warning(tmp_path):
    """Case 1. The acceptance criterion, in the form the ticket states it."""
    report = V.validate_source(generated(tmp_path, schema_drift="add_column"))

    assert not report.incompatible
    assert len(report.findings) == 1, report.findings

    finding = report.findings[0]
    assert finding.severity == "warning"
    assert finding.kind == "added_column"
    assert finding.table == "gl_entry"
    assert schema.DRIFT_COLUMN in finding.message
    assert "gl_entry" in finding.message

    assert report.warnings == report.findings

    for table in TABLES:
        if table != "gl_entry":
            assert table_report(report, table).findings == []


def test_a_dropped_column_fails_the_run(tmp_path):
    """Case 2. The other half of the acceptance criterion."""
    report = V.validate_source(generated(tmp_path, schema_drift="drop_column"))

    assert report.incompatible
    missing = [f for f in report.findings if f.kind == "missing_column"]
    assert len(missing) == 1, report.findings
    assert missing[0].severity == "incompatible"
    assert missing[0].table == "gl_entry"
    assert "currency" in missing[0].message


# --- Cases 3-9: header classification --------------------------------------

@pytest.mark.parametrize("table", TABLES)
def test_an_added_column_is_compatible_in_every_table(tmp_path, table):
    """Case 3."""
    report = V.validate_source(
        generated(tmp_path, schema_drift="add_column", schema_drift_table=table)
    )
    assert not report.incompatible
    assert kinds(report) == ["added_column"]
    assert report.findings[0].table == table
    assert schema.DRIFT_COLUMN in report.findings[0].message


@pytest.mark.parametrize("table", TABLES)
def test_a_dropped_column_is_incompatible_in_every_table(tmp_path, table):
    """Case 4."""
    report = V.validate_source(
        generated(tmp_path, schema_drift="drop_column", schema_drift_table=table)
    )
    assert report.incompatible
    dropped = schema.DRIFT_DROP_COLUMN[table]
    missing = [f for f in report.findings if f.kind == "missing_column"]
    assert [f.table for f in missing] == [table]
    assert dropped in missing[0].message


def test_reordering_the_declared_columns_is_incompatible(tmp_path):
    """Case 5. A swap of amount_dr and amount_cr reverses every debit and credit,
    and a check that compared only the set of names would pass it."""
    declared = list(schema.COLUMNS["gl_entry"])
    swapped = declared.copy()
    left, right = swapped.index("amount_dr"), swapped.index("amount_cr")
    swapped[left], swapped[right] = swapped[right], swapped[left]

    report = report_for(tmp_path, "gl_entry", [entry()], header=swapped)

    assert report.incompatible
    assert "column_order" in kinds(report)


def test_a_column_added_in_the_middle_is_still_an_addition(tmp_path):
    """Case 6. The declared columns are still in their declared relative order, so
    this is a producer adding a field, not a producer rearranging one."""
    declared = list(schema.COLUMNS["gl_entry"])
    header = declared[:4] + [schema.DRIFT_COLUMN] + declared[4:]
    row = entry(**{schema.DRIFT_COLUMN: schema.DRIFT_VALUE})

    report = report_for(tmp_path, "gl_entry", [row], header=header)

    assert not report.incompatible
    assert kinds(report) == ["added_column"]
    assert schema.DRIFT_COLUMN in report.findings[0].message


def test_a_repeated_header_column_is_incompatible(tmp_path):
    """Case 7."""
    declared = list(schema.COLUMNS["gl_entry"])
    header = declared + ["currency"]
    directory = tmp_path / "dupe"
    directory.mkdir()
    (directory / "gl_entry.csv").write_text(
        ",".join(header) + "\n" + ",".join([GL_ENTRY[c] for c in declared] + ["CNY"]) + "\n",
        encoding="utf-8",
    )

    report = V.validate_source(directory, tables=["gl_entry"])

    assert report.incompatible
    assert "duplicate_column" in kinds(report)


def test_a_repeated_unknown_column_is_reported_alone(tmp_path):
    """Case 7b. A header that names the same unknown column twice is classified as the
    duplicate and nothing else - no added_column warning alongside it.

    This is deliberate. Which of two identically named columns any later check meant is
    undecidable, so every other reading of that header is guesswork, and the run fails
    on the duplicate regardless. A warning about a compatible change, attached to a
    header nobody can read, is not information. Case 42 is the contrast: a header that
    loses one column and gains another reports both, because nothing there is
    ambiguous."""
    contract = {
        "table": "t",
        "primary_key": ["a"],
        "columns": [
            {"name": "a", "type": "string", "nullable": False},
            {"name": "b", "type": "string", "nullable": False},
        ],
    }
    assert [kind for kind, _ in V.classify_header(contract, ["a", "x", "x", "b"])] == [
        "duplicate_column"
    ]


def test_a_missing_source_file_is_incompatible(tmp_path):
    """Case 8. A table that vanished is not a table that is fine."""
    directory = tmp_path / "partial"
    write_csv(directory, "gl_entry", schema.COLUMNS["gl_entry"], [entry()])

    report = V.validate_source(directory, tables=["gl_entry", "fx_rate"])

    assert report.incompatible
    missing = [f for f in report.findings if f.kind == "missing_table"]
    assert [f.table for f in missing] == ["fx_rate"]
    assert "fx_rate" in missing[0].message
    assert str(directory / "fx_rate.csv") in missing[0].message


def test_an_incompatible_header_stops_the_table(tmp_path):
    """Case 9. Every row also lacks the column, and reporting that would bury the one
    finding that matters under one message per row."""
    declared = [c for c in schema.COLUMNS["gl_entry"] if c != "currency"]
    rows = [entry(entry_id=f"E{n}") for n in range(50)]

    report = report_for(tmp_path, "gl_entry", rows, header=declared)

    assert kinds(report) == ["missing_column"]
    assert table_report(report, "gl_entry").rows_read == 0


# --- Cases 10-11: the gate does not cry wolf -------------------------------

def test_a_header_that_both_loses_and_gains_a_column(tmp_path):
    """Case 42. The two classifications are independent, and a producer that renames a
    column produces exactly this: one gone, one arrived. It fails - a rename is not an
    addition - but the warning about the new column is reported alongside, because the
    two together are what tell someone it was a rename."""
    declared = [c for c in schema.COLUMNS["gl_entry"] if c != "currency"]
    header = declared + ["ccy"]
    rows = [{**entry(), "ccy": "CNY"}]

    report = report_for(tmp_path, "gl_entry", rows, header=header)

    assert report.incompatible
    assert set(kinds(report)) == {"missing_column", "added_column"}
    assert any("currency" in f.message for f in report.findings)
    assert any("ccy" in f.message for f in report.findings)


def test_the_clean_baseline_produces_no_findings(tmp_path):
    """Case 10."""
    report = V.validate_source(generated(tmp_path))
    assert report.findings == []
    assert not report.incompatible


def test_every_business_switch_on_produces_no_findings(tmp_path):
    """Case 11. These switches produce legitimate business events — a late entry, a
    restatement, a cost centre that changed department. A gate that called them
    violations would be switched off within a month.

    schema_drift is excluded: violating the column contract is the point of it.
    """
    out = generated(tmp_path, **{switch: True for switch in BUSINESS_SWITCHES})
    report = V.validate_source(out)
    assert report.findings == [], report.findings


# --- Cases 12-20: the value rules ------------------------------------------

def test_an_unparseable_amount_is_reported_and_does_not_crash(tmp_path):
    """Case 12. exactly_one_nonzero evaluates Decimal(row['amount_dr']), which raises
    on 'abc'. The constraint is skipped for this row rather than crashing: a rule
    about two values cannot be judged when one of them is not a value."""
    report = report_for(tmp_path, "gl_entry", [entry(amount_dr="abc")])

    assert report.incompatible
    assert kinds(report) == ["value"]
    assert "amount_dr" in report.findings[0].message
    assert "row 1" in report.findings[0].message


def test_a_row_constraint_reports_rather_than_raises_on_a_bad_value():
    """Case 12b. The guard does not depend on check_row being the only caller."""
    constraint = {"type": "exactly_one_nonzero", "columns": ["amount_dr", "amount_cr"]}
    problem = V.check_row_constraint(constraint, entry(amount_dr="abc"))
    assert problem and "amount_dr" in problem


NOT_AFTER = {"type": "not_after", "earlier": "accounting_date", "later": "posted_at"}
ONE_NONZERO = {"type": "exactly_one_nonzero", "columns": ["amount_dr", "amount_cr"]}


@pytest.mark.parametrize("posted_at", ["not-a-date", "2026-02-31", "2026-1-5"])
def test_not_after_reports_a_value_it_cannot_compare(posted_at):
    """Case 12c. The comparison is lexicographic, which equals comparing dates only
    while both values are real dates. Each of these sorts after 2026-01-01, so without
    the guard the rule quietly passes the row it exists to catch - and `2026-02-31` is
    the one that matters, because it has the right shape and no thirty-first of
    February behind it."""
    problem = V.check_row_constraint(
        NOT_AFTER, entry(accounting_date="2026-01-01", posted_at=posted_at)
    )
    assert problem and "posted_at" in problem, problem


@pytest.mark.parametrize("constraint", [NOT_AFTER, ONE_NONZERO])
@pytest.mark.parametrize("row", [
    {},
    {"amount_dr": None, "amount_cr": "0.00"},
    # None in not_after's own columns: the amounts being present is what made the
    # previous row prove nothing about this branch.
    {"accounting_date": None, "posted_at": "2026-01-01",
     "amount_dr": None, "amount_cr": "0.00"},
])
def test_a_row_constraint_reports_a_column_it_cannot_read(constraint, row):
    """Case 12d. check_row never hands it a row like this - it marks such a column
    unusable and skips the constraint - but a direct caller can, and the answer must
    be a message rather than a KeyError or a TypeError out of the validator."""
    problem = V.check_row_constraint(constraint, row)
    assert problem, f"{constraint['type']} said nothing about {row}"


def test_an_empty_value_in_a_not_null_column_is_incompatible(tmp_path):
    """Case 13. An empty field is the null value; the contract says currency is not."""
    report = report_for(tmp_path, "gl_entry", [entry(currency="")])
    assert report.incompatible
    assert kinds(report) == ["null"]


def test_an_empty_value_in_a_nullable_column_is_fine(tmp_path):
    """Case 14. parent_code is declared nullable, and a top-level account has none."""
    report = report_for(
        tmp_path, "dim_account_src", [{**ACCOUNT, "parent_code": ""}]
    )
    assert report.findings == []


def test_a_value_outside_allowed_is_incompatible(tmp_path):
    """Case 15."""
    report = report_for(tmp_path, "gl_entry", [entry(currency="JPY")])
    assert report.incompatible
    assert kinds(report) == ["value"]
    assert "JPY" in report.findings[0].message


def test_a_value_below_the_minimum_is_incompatible(tmp_path):
    """Case 16. version is declared min: 1."""
    report = report_for(tmp_path, "gl_entry", [entry(version="0")])
    assert report.incompatible
    assert kinds(report) == ["value"]


def test_a_value_at_the_wrong_scale_is_incompatible(tmp_path):
    """Case 17. Amounts are written to two places, always."""
    report = report_for(tmp_path, "gl_entry", [entry(amount_dr="1.5")])
    assert report.incompatible
    assert kinds(report) == ["value"]


def test_a_repeated_primary_key_is_incompatible(tmp_path):
    """Case 18. A repeated (entry_id, version) makes the idempotent merge produce a
    wrong answer rather than a visible duplicate."""
    rows = [entry(), entry(doc_id="D2")]
    report = report_for(tmp_path, "gl_entry", rows)

    assert report.incompatible
    assert "duplicate_key" in kinds(report)


def test_a_broken_not_after_constraint_is_incompatible(tmp_path):
    """Case 19."""
    report = report_for(
        tmp_path, "gl_entry", [entry(accounting_date="2026-03-01", posted_at="2026-02-03")]
    )
    assert report.incompatible
    assert "row_constraint" in kinds(report)
    assert any("not_after" in f.message for f in report.findings)


def test_a_broken_exactly_one_nonzero_constraint_is_incompatible(tmp_path):
    """Case 20. One row is one side of a voucher: a debit or a credit, never both."""
    report = report_for(tmp_path, "gl_entry", [entry(amount_dr="100.00", amount_cr="40.00")])
    assert report.incompatible
    assert "row_constraint" in kinds(report)
    assert any("exactly_one_nonzero" in f.message for f in report.findings)


# --- Cases 21-27: the report -----------------------------------------------

def bad_currency_rows(count: int) -> list[dict]:
    """Rows valid in every respect except currency: one finding each, no other."""
    return [entry(entry_id=f"E{n}", currency="JPY") for n in range(count)]


def test_findings_are_capped_and_the_remainder_counted_exactly(tmp_path):
    """Case 21. Capping alone understates the damage and invites 'only fifty rows are
    bad, ship it'; counting alone gives an accurate number nobody can read."""
    rows = bad_currency_rows(120)

    capped = report_for(tmp_path, "gl_entry", rows, max_findings=50)
    assert len(capped.findings) == 50
    assert table_report(capped, "gl_entry").findings_omitted == 70

    # The same file, uncapped: 120 was the real number, not a floor.
    whole = report_for(tmp_path, "gl_entry", rows, max_findings=200)
    assert len(whole.findings) == 120
    assert table_report(whole, "gl_entry").findings_omitted == 0


def test_the_cap_never_hides_a_header_finding(tmp_path):
    """Case 22. A cap that could hide 'the currency column is gone' defeats the gate."""
    declared = [c for c in schema.COLUMNS["gl_entry"] if c != "currency"]
    report = report_for(tmp_path, "gl_entry", [entry()], header=declared, max_findings=0)

    assert report.incompatible
    assert kinds(report) == ["missing_column"]


def test_the_cap_never_decides_whether_the_run_fails(tmp_path):
    """Case 22b. The cap governs how much is reported, never the verdict. Reading the
    verdict off the collected findings would make --max-findings 0 exit 0 on a table
    where every row is broken: a gate that reports nothing and so passes everything."""
    directory = one_table(tmp_path, "gl_entry", bad_currency_rows(3))

    report = V.validate_source(directory, tables=["gl_entry"], max_findings=0)
    assert report.findings == []
    assert table_report(report, "gl_entry").findings_omitted == 3
    assert report.incompatible

    result = run_cli("--source", str(directory), "--table", "gl_entry", "--max-findings", "0")
    assert result.returncode == 1, result.stderr


def test_an_empty_file_carries_none_of_the_declared_columns(tmp_path):
    """Case 40. A truncated extract is not an empty table."""
    directory = tmp_path / "empty"
    directory.mkdir()
    (directory / "gl_entry.csv").write_text("", encoding="utf-8")

    report = V.validate_source(directory, tables=["gl_entry"])

    assert report.incompatible
    assert kinds(report) == ["missing_column"]
    assert table_report(report, "gl_entry").rows_read == 0


# Run in a subprocess so the exit code and the stderr are the real ones a caller
# sees, not an in-process return value that could be right while the output is not.
CONTRACT_ERROR_PROGRAM = """
import sys
from pathlib import Path
from ingest import contracts, validate
contracts.CONTRACT_DIR = Path(sys.argv[1])
contracts.load.cache_clear()
sys.exit(validate.main(["--source", sys.argv[2], "--table", "gl_entry"]))
"""


def test_a_broken_contract_is_a_usage_error(tmp_path, monkeypatch):
    """Case 41. A contract that does not load is not about the data, so it raises
    rather than becoming a finding - and the command reports it as a usage error."""
    monkeypatch.setattr(contracts, "CONTRACT_DIR", tmp_path / "contracts")
    (tmp_path / "contracts").mkdir()
    (tmp_path / "contracts" / "gl_entry.yaml").write_text(
        "table: gl_entry\nprimary_key: [a]\ncolumns: []\n", encoding="utf-8"
    )
    contracts.load.cache_clear()

    source = one_table(tmp_path, "gl_entry", [entry()])
    try:
        with pytest.raises(contracts.ContractError):
            V.validate_source(source, tables=["gl_entry"])

        result = subprocess.run(
            [sys.executable, "-c", CONTRACT_ERROR_PROGRAM, str(tmp_path / "contracts"),
             str(source)],
            capture_output=True, text=True, cwd=REPO_ROOT,
        )
        assert result.returncode == 2, result.stderr
        assert "columns" in result.stderr
        assert "Traceback" not in result.stderr
    finally:
        contracts.load.cache_clear()


def test_a_table_with_no_contract_is_a_usage_error(tmp_path):
    """Case 44. Distinct from a contract that is malformed: this one is not there at
    all, and asking to validate a table nobody has written a contract for is a mistake
    in the request, not a finding about the data."""
    source = one_table(tmp_path, "gl_entry", [entry()])

    with pytest.raises(contracts.ContractError):
        V.validate_source(source, tables=["no_such_table"])

    assert V.main(["--source", str(source), "--table", "no_such_table"]) == 2


def test_an_unknown_row_constraint_raises(tmp_path):
    """Case 45. The loader rejects an undeclared constraint type, so reaching this is
    a broken invariant rather than bad data - and it raises rather than reporting,
    because a rule the validator cannot evaluate must not read as a rule that passed."""
    with pytest.raises(ValueError, match="unknown row constraint"):
        V.check_row_constraint({"type": "invented"}, entry())


def test_every_table_is_validated_even_after_one_fails(tmp_path):
    """Case 23. Upstream schema changes arrive in batches, because they come from one
    release of one source system."""
    directory = tmp_path / "two_broken"
    write_csv(directory, "gl_entry", schema.COLUMNS["gl_entry"], [entry(currency="JPY")])
    write_csv(
        directory,
        "fx_rate",
        [c for c in schema.COLUMNS["fx_rate"] if c != "rate_to_base"],
        [FX_RATE],
    )

    report = V.validate_source(directory, tables=["gl_entry", "fx_rate"])

    assert report.incompatible
    assert table_report(report, "gl_entry").findings != []
    assert table_report(report, "fx_rate").findings != []


def test_rows_read_counts_the_data_rows(tmp_path):
    """Case 24."""
    report = report_for(tmp_path, "gl_entry", [entry(entry_id=f"E{n}") for n in range(7)])
    assert table_report(report, "gl_entry").rows_read == 7


def test_a_warning_is_logged(tmp_path, caplog):
    """Case 25. The structured finding is what callers read; the log line is what a
    person reads, and both are required."""
    out = generated(tmp_path, schema_drift="add_column")
    with caplog.at_level(logging.WARNING, logger="ingest.validate"):
        V.validate_source(out)

    records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(records) == 1, [r.getMessage() for r in records]
    assert schema.DRIFT_COLUMN in records[0].getMessage()


def test_an_incompatible_finding_is_logged_at_error(tmp_path, caplog):
    """Case 26. From row validation, not from a setup or CLI failure."""
    with caplog.at_level(logging.ERROR, logger="ingest.validate"):
        report_for(tmp_path, "gl_entry", [entry(currency="JPY")])

    records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(records) == 1, [r.getMessage() for r in records]
    assert "gl_entry" in records[0].getMessage()
    assert "currency" in records[0].getMessage()


def test_the_library_configures_no_logging(tmp_path):
    """Case 27. Handlers, formatting and levels belong to whoever is running it."""
    logger = logging.getLogger("ingest.validate")
    assert logger.handlers == []
    # NOTSET as well: a library that pins its own level has configured logging just as
    # surely as one that installs a handler, and it overrides the application.
    assert logger.level == logging.NOTSET

    V.validate_source(generated(tmp_path))
    after = logging.getLogger("ingest.validate")
    assert after.handlers == []
    assert after.level == logging.NOTSET


# --- Cases 28-29: downstream impact ----------------------------------------

def test_a_failure_states_that_downstream_impact_is_unknown(tmp_path):
    """Case 28. dbt has not landed, so there is no lineage graph and no models to
    name. Saying so is what keeps the unfinished half of the requirement visible."""
    report = report_for(tmp_path, "gl_entry", [entry(currency="JPY")])
    described = report.describe()

    assert "Downstream impact:" in described
    assert "unknown" in described
    assert "dbt" in described


def test_a_passing_report_carries_no_downstream_section(tmp_path):
    """Case 29. Neither a clean run nor a warning-only run."""
    clean = V.validate_source(generated(tmp_path))
    assert "Downstream impact:" not in clean.describe()

    warned = V.validate_source(generated(tmp_path, schema_drift="add_column"))
    assert not warned.incompatible
    assert "Downstream impact:" not in warned.describe()


# --- Cases 30-35: the command ----------------------------------------------

def run_cli(*args) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "ingest.validate", *args],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )


def test_the_command_exits_zero_on_clean_data(tmp_path):
    """Case 30."""
    result = run_cli("--source", str(generated(tmp_path)))
    assert result.returncode == 0, result.stderr


def test_the_command_exits_zero_and_warns_on_an_added_column(tmp_path):
    """Case 31."""
    result = run_cli("--source", str(generated(tmp_path, schema_drift="add_column")))
    assert result.returncode == 0, result.stderr
    assert schema.DRIFT_COLUMN in result.stderr


def test_the_command_fails_on_a_dropped_column(tmp_path):
    """Case 32. The run stops, and the message says what broke and what is not yet
    knowable about the damage."""
    result = run_cli("--source", str(generated(tmp_path, schema_drift="drop_column")))

    assert result.returncode == 1
    assert "currency" in result.stderr
    assert "gl_entry" in result.stderr
    assert "Downstream impact:" in result.stderr


def test_the_table_option_scopes_the_run(tmp_path):
    """Case 33. Exit 0 has to come from scoping, not from validating nothing — so the
    same source without --table must fail."""
    out = generated(tmp_path, schema_drift="drop_column", schema_drift_table="fx_rate")

    scoped = run_cli("--source", str(out), "--table", "gl_entry")
    assert scoped.returncode == 0, scoped.stderr

    whole = run_cli("--source", str(out))
    assert whole.returncode == 1

    report = V.validate_source(out, tables=["gl_entry"])
    assert [t.table for t in report.tables] == ["gl_entry"]
    assert table_report(report, "gl_entry").rows_read > 0


def test_the_command_honours_the_findings_cap(tmp_path):
    """Case 34."""
    directory = one_table(tmp_path, "gl_entry", bad_currency_rows(120))

    result = run_cli("--source", str(directory), "--table", "gl_entry", "--max-findings", "5")

    assert result.returncode == 1
    assert result.stderr.count("JPY") == 5, result.stderr
    assert "gl_entry: 115 more findings not shown" in result.stderr


def test_an_unknown_option_is_a_usage_error():
    """Case 43. argparse owns this exit code; asserting it means a later hand-rolled
    parser cannot quietly start returning something else."""
    result = run_cli("--nonsense")
    assert result.returncode == 2
    assert "Traceback" not in result.stderr

    # And as a function: main promises an int, so it must not raise SystemExit at a
    # caller that is not the process.
    assert V.main(["--nonsense"]) == 2


def test_a_missing_source_directory_is_a_usage_error(tmp_path):
    """Case 35. A message naming the directory, not a traceback."""
    absent = tmp_path / "not_here"
    result = run_cli("--source", str(absent))

    assert result.returncode == 2
    assert "not_here" in result.stderr
    assert "Traceback" not in result.stderr


# --- Cases 36-39: boundaries and scale -------------------------------------

WIDE_CONTRACT = {
    "table": "wide",
    "primary_key": ["k"],
    "columns": [
        {"name": "k", "type": "string", "nullable": False},
        {"name": "pad", "type": "string", "nullable": False},
    ],
}


def wide_table(path: Path, rows: int, width: int, distinct_keys: int | None = None) -> Path:
    distinct = rows if distinct_keys is None else distinct_keys
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["k", "pad"])
        for n in range(rows):
            writer.writerow([f"k{n % distinct}", "x" * width])
    return path


def peak_bytes(contract, path: Path) -> int:
    tracemalloc.start()
    V.validate_table(contract, path, max_findings=50)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak


def test_peak_memory_does_not_track_row_width(tmp_path):
    """Case 36. Row width is the term that would explode if rows were being collected,
    and it is the term the primary-key set does not touch — which is what makes
    streaming measurable here at all. See docs/adr/0011."""
    narrow = peak_bytes(WIDE_CONTRACT, wide_table(tmp_path / "narrow.csv", 50_000, 20))
    wide = peak_bytes(WIDE_CONTRACT, wide_table(tmp_path / "wide.csv", 50_000, 60))

    assert wide < narrow * 1.2, (
        f"tripling the row width took {wide / narrow:.2f}x the peak memory "
        f"({narrow} -> {wide} bytes); rows are being collected, not streamed"
    )


def test_peak_memory_does_track_distinct_primary_keys(tmp_path):
    """Case 36b. The one exception ADR 0011 records, asserted rather than pretended
    away, so the scale ticket finds the cost already stated."""
    many = peak_bytes(WIDE_CONTRACT, wide_table(tmp_path / "many.csv", 100_000, 20))
    few = peak_bytes(
        WIDE_CONTRACT, wide_table(tmp_path / "few.csv", 100_000, 20, distinct_keys=10)
    )

    assert many > few * 2, (
        f"100,000 distinct keys peaked at {many} bytes and 10 keys at {few}; the "
        f"primary-key set is supposed to be the term that scales"
    )


def test_the_validator_does_not_import_the_generator():
    """Case 37. ADR 0008's prohibition now covers the module that reads the data: a
    validator derived from its producer cannot catch the producer changing."""
    module = importlib.import_module("ingest.validate")
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    assert not {n for n in imported if n == "generator" or n.startswith("generator.")}

    # In a fresh process: this module imports the generator at the top, so an
    # in-process check can never see the validator pulling it in.
    program = (
        "import sys, importlib;"
        "importlib.import_module('ingest.validate');"
        "print(sorted(n for n in sys.modules if n.split('.')[0] == 'generator'))"
    )
    result = subprocess.run(
        [sys.executable, "-c", program],
        capture_output=True, text=True, check=True, cwd=REPO_ROOT,
    )
    assert result.stdout.strip() == "[]", (
        f"importing the validator pulled in {result.stdout.strip()}"
    )


def contracts_test_module() -> ast.Module:
    return ast.parse((REPO_ROOT / "tests" / "test_contracts.py").read_text(encoding="utf-8"))


def test_the_contract_tests_keep_no_copy_of_the_primitives():
    """Case 38. A copy is the thing that drifts, so the assertion is against the
    module's own namespace rather than against its behaviour."""
    tree = contracts_test_module()

    defined = {
        node.name for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    defined |= {
        target.id
        for node in tree.body if isinstance(node, ast.Assign)
        for target in node.targets if isinstance(target, ast.Name)
    }
    for name in ("violations", "check_row", "check_value", "check_row_constraint", "is_null"):
        assert name not in defined, f"tests/test_contracts.py still defines {name!r}"

    imported_from_validator = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "ingest.validate"
        for alias in node.names
    }
    assert {"check_value", "check_row_constraint"} <= imported_from_validator

    # The objects themselves, not just an import statement that could sit above a
    # later rebinding.
    if str(REPO_ROOT / "tests") not in sys.path:
        sys.path.insert(0, str(REPO_ROOT / "tests"))
    test_contracts = importlib.import_module("test_contracts")
    assert test_contracts.check_value is V.check_value
    assert test_contracts.check_row_constraint is V.check_row_constraint


def test_the_contract_tests_go_through_the_validator():
    """Case 39. Cases 7 and 8 assert what they always asserted, now against production
    rather than against a rule executor that lived only in the test file."""
    tree = contracts_test_module()

    for name in (
        "test_the_contracts_hold_for_clean_data",
        "test_the_contracts_hold_with_every_business_switch_on",
    ):
        function = next(
            (n for n in tree.body
             if isinstance(n, ast.FunctionDef) and n.name == name), None
        )
        assert function is not None, f"tests/test_contracts.py lost {name}"
        called = {
            node.func.id for node in ast.walk(function)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        } | {
            node.func.attr for node in ast.walk(function)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        assert "validate_source" in called, f"{name} does not go through the validator"
        assert "violations" not in called, f"{name} still uses the local rule executor"
