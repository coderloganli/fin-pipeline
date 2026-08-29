"""Applying a contract to a source table: the first of the three quality gates.

`ingest/contracts/` says what ingest expects each source table to look like. This
module says what happens when reality disagrees with it, and that rule is one rule for
all five tables rather than something each contract restates:

**An added column is compatible.** A reader that does not know about a new column is
unaffected by it. The run continues, and a warning is recorded so that someone decides
whether the new field belongs in the contract before something starts depending on it.

**Everything else fails the run.** A column that has gone, columns that have been
reordered, a value that no longer parses as its declared type, an empty field in a
column declared not null, a value outside `allowed` or below `min` or at the wrong
`scale`, a repeated primary key, a broken row constraint. A reader that depends on any
of those is already broken; the only question is whether it finds out here or three
layers downstream in a number nobody double-checks.

Nothing here imports the generator, for the reason the contracts do not either: a
check derived from its producer cannot catch the producer changing. See
docs/adr/0008-contracts-are-written-by-hand.md.

See also docs/adr/0009-what-counts-as-a-compatible-schema-change.md,
docs/adr/0010-the-validator-owns-the-rule-primitives.md,
docs/adr/0011-validation-streams-and-caps-its-findings.md and
docs/adr/0012-the-validator-reports-rather-than-raises.md.
"""

import argparse
import csv
import logging
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from . import contracts

__all__ = [
    "Finding",
    "readable",
    "TableReport",
    "Report",
    "check_value",
    "check_row_constraint",
    "check_row",
    "classify_header",
    "validate_table",
    "validate_source",
    "downstream_impact",
    "main",
]

LOGGER = logging.getLogger("ingest.validate")

DEFAULT_MAX_FINDINGS = 50
DEFAULT_SOURCE = Path("data/source")

# The loader's definitions, not copies: a second definition here could drift from the
# contracts' own statement of what an empty field and a date mean.
is_null = contracts.is_null
DATE_FORMAT = contracts.DATE_FORMAT

DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")

WARNING = "warning"
INCOMPATIBLE = "incompatible"

# Only one kind is a warning. See docs/adr/0009.
COMPATIBLE_KINDS = frozenset({"added_column"})

DOWNSTREAM_IMPACT = (
    "Downstream impact: unknown. No lineage graph exists yet - dbt has not landed, so "
    "the models that would break cannot be named. See docs/adr/0012."
)


# --- what a finding is -----------------------------------------------------

@dataclass(frozen=True)
class Finding:
    """One disagreement between a contract and the data.

    `kind` is what a caller branches on and `message` is what a person reads. Both,
    because the run record will want the first and a DAG log will want the second.
    """

    table: str
    severity: str
    kind: str
    message: str


@dataclass
class TableReport:
    table: str
    findings: list[Finding] = field(default_factory=list)
    rows_read: int = 0
    findings_omitted: int = 0
    incompatible_omitted: int = 0

    @property
    def incompatible(self) -> bool:
        """Whether this table fails the run.

        The cap governs how much is *reported*, never whether the run fails. Reading
        this off the collected findings alone would mean `--max-findings 0` exits 0
        on a table where every row is broken - a gate that reports nothing and
        therefore passes everything.
        """
        return (
            any(finding.severity == INCOMPATIBLE for finding in self.findings)
            or self.incompatible_omitted > 0
        )


@dataclass
class Report:
    tables: list[TableReport] = field(default_factory=list)

    @property
    def findings(self) -> list[Finding]:
        return [finding for table in self.tables for finding in table.findings]

    @property
    def warnings(self) -> list[Finding]:
        return [finding for finding in self.findings if finding.severity == WARNING]

    @property
    def incompatible(self) -> bool:
        return any(table.incompatible for table in self.tables)

    def describe(self) -> str:
        """The text a person reads. Carries the downstream section only when the run
        is going to fail, because that is the only time the missing lineage matters."""
        lines: list[str] = []
        for table in self.tables:
            for finding in table.findings:
                lines.append(f"[{finding.severity}] {finding.message}")
            if table.findings_omitted:
                lines.append(
                    f"{table.table}: {table.findings_omitted} more findings not shown"
                )
        if not lines:
            lines.append("Contract validation passed: no findings.")
        if self.incompatible:
            lines.append("")
            lines.append(
                downstream_impact([t.table for t in self.tables if t.incompatible])
            )
        return "\n".join(lines)


def downstream_impact(tables: list[str] | None = None) -> str:
    """The models that would break, once there is a lineage graph to ask.

    There is not one yet, so this states that rather than returning nothing: an empty
    section would let the unfinished half of the requirement pass for finished. The
    dbt ticket replaces the body of this function and nothing else - which is why it
    already takes the tables that changed, the argument it will need and this
    implementation has no use for.
    """
    return DOWNSTREAM_IMPACT


# --- the rule primitives ---------------------------------------------------

def check_value(spec: dict, value: str) -> str | None:
    """Check one non-null field against its column spec. None means it is fine."""
    kind = spec["type"]

    if kind == "date":
        # The pattern first: strptime alone accepts "2026-1-5", which is not the
        # format the contracts declare and not what the pipeline parses downstream.
        if not DATE_PATTERN.fullmatch(value):
            return f"{value!r} is not a date in {DATE_FORMAT}"
        try:
            datetime.strptime(value, DATE_FORMAT)
        except ValueError:
            return f"{value!r} is not a real date"
        return None

    if kind == "integer":
        if not value.lstrip("-").isdigit():
            return f"{value!r} is not an integer"
        number = Decimal(value)
    elif kind == "decimal":
        try:
            number = Decimal(value)
        except InvalidOperation:
            return f"{value!r} is not a number"
        # NaN and Infinity parse happily, and comparing NaN raises rather than
        # returning False - so the validator would crash instead of reporting.
        if not number.is_finite():
            return f"{value!r} is not a finite number"
        if "scale" in spec:
            fraction = value.partition(".")[2]
            if len(fraction) != spec["scale"]:
                return f"{value!r} is not written to {spec['scale']} decimal places"
    else:  # string
        number = None

    if number is not None and "min" in spec and number < spec["min"]:
        return f"{value!r} is below the minimum {spec['min']}"
    if "allowed" in spec and value not in spec["allowed"]:
        return f"{value!r} is not one of {spec['allowed']}"
    return None


def constraint_columns(constraint: dict) -> list[str]:
    """The columns a row constraint reads."""
    if constraint["type"] == "exactly_one_nonzero":
        return list(constraint["columns"])
    return [constraint["earlier"], constraint["later"]]


def readable(row: dict[str, str], name: str) -> tuple[str | None, str | None]:
    """The value a constraint wants, or a sentence saying why there isn't one.

    A row constraint reads columns it did not check itself, and a caller reaching it
    directly may hand it a row missing one of them. Saying so is the job; raising
    KeyError out of the validator is not.
    """
    value = row.get(name)
    if value is None:
        return None, f"{name} is missing from the row"
    if not isinstance(value, str):
        return None, f"{name} ({value!r}) is not a text field"
    return value, None


def check_row_constraint(constraint: dict, row: dict[str, str]) -> str | None:
    """Check one cross-column rule against one row. None means it is fine.

    Reports an unparseable value rather than raising on it. `check_row` already skips
    a constraint whose columns failed their own checks, but this does not depend on
    being called only from there: a caller reaching it directly must get a message
    back, not an `InvalidOperation` out of the validator.
    """
    kind = constraint["type"]

    if kind == "not_after":
        earlier, later = constraint["earlier"], constraint["later"]
        # The comparison is lexicographic, which equals comparing dates only while both
        # values are real dates in the declared format. On anything else it silently
        # returns an answer rather than raising, which is worse than a crash:
        # `not-a-date` sorts after every real date, and so does `2026-02-31`, which has
        # the right shape and no thirty-first of February behind it.
        for name in (earlier, later):
            value, unusable = readable(row, name)
            if unusable:
                return f"not_after: {unusable}, so the ordering cannot be judged"
            problem = check_value({"type": "date"}, value)
            if problem:
                return (
                    f"not_after: {name} is not usable as a date ({problem}), so the "
                    f"ordering cannot be judged"
                )
        if row[later] < row[earlier]:
            return (
                f"not_after: {later} ({row[later]}) is before {earlier} ({row[earlier]})"
            )
        return None

    if kind == "exactly_one_nonzero":
        columns = constraint["columns"]
        nonzero = []
        for name in columns:
            value, unusable = readable(row, name)
            if unusable:
                return f"exactly_one_nonzero: {unusable}"
            try:
                number = Decimal(value)
            except InvalidOperation:
                return f"exactly_one_nonzero: {name} ({value!r}) is not a number"
            if not number.is_finite():
                return f"exactly_one_nonzero: {name} ({value!r}) is not finite"
            if number != 0:
                nonzero.append(name)
        if len(nonzero) != 1:
            return (
                f"exactly_one_nonzero: exactly one of {columns} should be non-zero, "
                f"found {nonzero}"
            )
        return None

    raise ValueError(f"unknown row constraint {kind!r}")


def check_row(contract: dict, row: dict[str, str]) -> list[tuple[str, str, str]]:
    """Check one row against a contract. Returns `(kind, column, problem)` triples,
    where `column` is empty for a rule that spans columns rather than qualifying one.

    A row constraint is skipped when a column it reads already failed its own check.
    A rule about the relationship between two values cannot be evaluated when one of
    them is not a value: the finding already raised about the column is the diagnosis,
    and a second message about the constraint would be noise laid over a crash that
    had to be avoided anyway. See docs/adr/0010.
    """
    problems: list[tuple[str, str, str]] = []
    unusable: set[str] = set()

    for spec in contract["columns"]:
        name = spec["name"]
        value = row.get(name)
        if value is None:
            unusable.add(name)
            problems.append(("missing_column", name, "missing from the row"))
            continue
        if is_null(value):
            # Null is unusable to a constraint whether or not it is permitted here.
            unusable.add(name)
            if not spec["nullable"]:
                problems.append(
                    ("null", name, "empty, but the contract declares it not null")
                )
            continue
        problem = check_value(spec, value)
        if problem:
            unusable.add(name)
            problems.append(("value", name, problem))

    for constraint in contract.get("row_constraints", []):
        if unusable.intersection(constraint_columns(constraint)):
            continue
        problem = check_row_constraint(constraint, row)
        if problem:
            problems.append(("row_constraint", "", problem))

    return problems


def classify_header(contract: dict, header: list[str]) -> list[tuple[str, str]]:
    """Classify a header against a contract. Returns `(kind, message)` pairs.

    The declared columns must all be present and must appear in their declared
    *relative* order. Anything else in the header is an addition, wherever it sits.
    Relative order rather than absolute position: a producer that inserts a field in
    the middle has added a column, but one that swaps `amount_dr` and `amount_cr` has
    reversed every debit and credit. See docs/adr/0009.
    """
    table = contract["table"]
    declared = [spec["name"] for spec in contract["columns"]]

    duplicates = sorted({name for name in header if header.count(name) > 1})
    if duplicates:
        # Nothing further can be said about a header that names a column twice: which
        # of the two any later check meant is undecidable.
        return [
            (
                "duplicate_column",
                f"{table}: the header names {duplicates} more than once",
            )
        ]

    problems: list[tuple[str, str]] = []

    missing = [name for name in declared if name not in header]
    if missing:
        problems.append(
            (
                "missing_column",
                f"{table}: the contract declares {missing}, which the header does "
                f"not carry",
            )
        )

    in_header = [name for name in header if name in declared]
    in_contract = [name for name in declared if name in header]
    if in_header != in_contract:
        problems.append(
            (
                "column_order",
                f"{table}: the declared columns appear as {in_header}, but the "
                f"contract states {in_contract}",
            )
        )

    for name in header:
        if name not in declared:
            problems.append(
                (
                    "added_column",
                    f"{table}: the header carries {name!r}, which the contract does "
                    f"not declare; an added column is compatible, so the run continues",
                )
            )

    return problems


# --- the walk --------------------------------------------------------------

def severity_of(kind: str) -> str:
    return WARNING if kind in COMPATIBLE_KINDS else INCOMPATIBLE


def validate_table(
    contract: dict,
    path: Path,
    *,
    max_findings: int = DEFAULT_MAX_FINDINGS,
    logger: logging.Logger | None = None,
) -> TableReport:
    """Validate one source file against one contract.

    Rows stream: each is checked and forgotten, so peak memory does not track row
    width. The primary-key set is the one thing held, and it is held deliberately -
    a repeated key makes the idempotent merge produce a wrong answer rather than a
    visible duplicate. See docs/adr/0011.
    """
    logger = logger or LOGGER
    table = contract["table"]
    report = TableReport(table=table)
    row_findings = 0

    def record(kind: str, message: str, *, capped: bool) -> None:
        nonlocal row_findings
        if capped:
            if row_findings >= max_findings:
                report.findings_omitted += 1
                if severity_of(kind) == INCOMPATIBLE:
                    report.incompatible_omitted += 1
                return
            row_findings += 1
        finding = Finding(table, severity_of(kind), kind, message)
        report.findings.append(finding)
        logger.log(
            logging.WARNING if finding.severity == WARNING else logging.ERROR,
            finding.message,
        )

    path = Path(path)
    if not path.is_file():
        record("missing_table", f"{table}: no source file at {path}", capped=False)
        return report

    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            record(
                "missing_column",
                f"{table}: the file is empty, so it carries none of the declared "
                f"columns",
                capped=False,
            )
            return report

        header_problems = classify_header(contract, header)
        for kind, message in header_problems:
            record(kind, message, capped=False)

        # A header that is already condemned stops the table. Reading on would bury
        # the one finding that matters under one violation per row.
        if any(severity_of(kind) == INCOMPATIBLE for kind, _ in header_problems):
            return report

        key_columns = contract["primary_key"]
        seen: set[tuple[str, ...]] = set()

        for values in reader:
            report.rows_read += 1
            row = dict(zip(header, values))

            for kind, column, problem in check_row(contract, row):
                where = f"{table}.{column}" if column else table
                record(kind, f"{where}: row {report.rows_read}: {problem}", capped=True)

            key = tuple(row.get(name, "") for name in key_columns)
            if key in seen:
                record(
                    "duplicate_key",
                    f"{table}: row {report.rows_read}: primary key {key} repeats",
                    capped=True,
                )
            seen.add(key)

    return report


def validate_source(
    source_dir,
    tables: list[str] | None = None,
    *,
    max_findings: int = DEFAULT_MAX_FINDINGS,
    logger: logging.Logger | None = None,
) -> Report:
    """Validate a source directory against the contracts.

    Every table is validated, including after one has failed: upstream schema changes
    arrive in batches, because they come from one release of one source system.

    Raises only for what is not about the data - a `source_dir` that is not a
    directory, a contract that does not load. Bad data is always a finding, because a
    warning and a failure have to reach the caller through the same call. See
    docs/adr/0012.
    """
    source = Path(source_dir)
    if not source.is_dir():
        raise NotADirectoryError(f"no source directory at {source}")

    names = list(tables) if tables is not None else contracts.tables()
    return Report(
        tables=[
            validate_table(
                contracts.load(name),
                source / f"{name}.csv",
                max_findings=max_findings,
                logger=logger,
            )
            for name in names
        ]
    )


# --- the command -----------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ingest.validate",
        description="Validate source extracts against the contracts in "
                    "ingest/contracts/. An added column warns; anything else fails.",
    )
    parser.add_argument("--source", default=str(DEFAULT_SOURCE),
                        help="directory holding the source CSVs (default: data/source)")
    parser.add_argument("--table", action="append", dest="tables", default=None,
                        help="validate only this table; repeatable")
    parser.add_argument("--max-findings", type=int, default=DEFAULT_MAX_FINDINGS,
                        help="findings collected per table before the rest are only "
                             "counted (default: 50)")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Turn a report into an exit code. The only place that knows about exit codes.

    Returns the code rather than raising it. argparse ends the process on a usage
    error, which is right when this is the process and wrong when it is a function
    something called - and the signature above promises an integer either way.
    """
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as ended:
        return int(ended.code or 0)

    # The application configures logging; the library never does.
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING, format="%(message)s")

    try:
        report = validate_source(
            args.source, tables=args.tables, max_findings=args.max_findings
        )
    except NotADirectoryError as failure:
        print(f"{failure}", file=sys.stderr)
        return 2
    except contracts.ContractError as failure:
        print(f"{failure}", file=sys.stderr)
        return 2

    # The findings themselves were logged as they were produced; this is the tail.
    for table in report.tables:
        if table.findings_omitted:
            print(
                f"{table.table}: {table.findings_omitted} more findings not shown",
                file=sys.stderr,
            )

    if report.incompatible:
        print("", file=sys.stderr)
        print(downstream_impact([t.table for t in report.tables]), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
