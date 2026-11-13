"""The source-table contracts.

A contract says what ingest expects a source table to look like. CSV carries no
schema of its own, so unless the expectation is written down somewhere the pipeline
can only believe whatever it reads.

Two shapes of assertion appear here, and the order matters. Cases 4-6 assert the
rules are *declared*; cases 7-8 assert the data satisfies them. Checking only the
second half would pass against contracts that declare no rules at all — a shape that
has now slipped through three tickets, so it is separated deliberately.
"""

import ast
import importlib
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from eval.generator import Config, dimensions, generate, schema
from ingest import contracts
from ingest.validate import check_value, check_row_constraint, validate_source

REPO_ROOT = Path(__file__).resolve().parent.parent
CONTRACT_DIR = REPO_ROOT / "ingest" / "contracts"

TABLES = tuple(schema.COLUMNS)

# Everything except schema_drift, whose whole purpose is to add or remove a column and
# which is therefore *meant* to violate the column contract.
BUSINESS_SWITCHES = (
    "late_entries",
    "restatements",
    "cost_centre_move",
    "unbalanced_vouchers",
    "growing_account",
    "amount_outliers",
    "long_tail_anomaly",
)

EXPECTED_PRIMARY_KEYS = {
    "gl_entry": ["entry_id", "version"],
    "gl_adjustment": ["entry_id", "version"],
    # Both dimensions are effective-dated: a cost centre that moves department has two
    # rows, which is the scenario the point-in-time join exists for, not a duplicate.
    "dim_account_src": ["account_code", "effective_date"],
    "dim_cost_center_src": ["cc_code", "effective_date"],
    "fx_rate": ["currency", "rate_date"],
}

CURRENCIES = ["CNY", "EUR", "USD", "GBP"]
ACCOUNT_TYPES = ["asset", "liability", "equity", "revenue", "expense"]


def column(contract: dict, name: str) -> dict:
    for entry in contract["columns"]:
        if entry["name"] == name:
            return entry
    raise AssertionError(f"{contract['table']} declares no column {name!r}")


def generated(tmp_path: Path, **switches) -> Path:
    out = tmp_path / f"data_{len(list(tmp_path.iterdir()))}"
    generate(Config(seed=42, out_dir=out, **switches))
    return out


# --- Cases 1-3: the contract files themselves ------------------------------

def test_there_is_a_contract_for_every_table_named_after_it():
    files = sorted(path.stem for path in CONTRACT_DIR.glob("*.yaml"))
    assert files == sorted(TABLES), (
        f"expected one contract per source table, got {files}"
    )
    for table in TABLES:
        assert contracts.load(table)["table"] == table, (
            f"{table}.yaml declares a different table name"
        )


def test_every_contract_is_structurally_valid():
    for table in TABLES:
        contract = contracts.load(table)   # load() validates, or raises

        # The permitted set itself is pinned by test_the_top_level_whitelist_did_not_widen;
        # this asserts each real contract stays inside it, whatever it currently is.
        assert set(contract) <= contracts.TOP_LEVEL
        assert contracts.REQUIRED <= set(contract)

        names = [spec["name"] for spec in contract["columns"]]
        assert len(names) == len(set(names)), f"{table} repeats a column name"

        assert len(contract["primary_key"]) == len(set(contract["primary_key"]))
        for key in contract["primary_key"]:
            assert key in names, f"{table} primary key names {key}, which is not a column"

        for spec in contract["columns"]:
            assert spec["type"] in contracts.TYPES
            assert isinstance(spec.get("nullable", False), bool)
            if "allowed" in spec:
                assert isinstance(spec["allowed"], list) and spec["allowed"]
            for modifier in ("min", "scale"):
                if modifier in spec:
                    assert modifier in contracts.MODIFIERS[spec["type"]], (
                        f"{table}.{spec['name']} is {spec['type']} and cannot take {modifier}"
                    )

        for constraint in contract.get("row_constraints", []):
            assert constraint["type"] in contracts.ROW_CONSTRAINTS


def test_the_columns_match_what_the_generator_emits():
    """Two independently written statements, compared. When they drift this goes red
    and a person decides which is wrong — that is the contract doing its job."""
    for table in TABLES:
        declared = [spec["name"] for spec in contracts.load(table)["columns"]]
        assert declared == list(schema.COLUMNS[table]), (
            f"{table}: contract and generator disagree on columns"
        )


# --- Cases 4-6: the rules are declared -------------------------------------

def test_the_business_rules_are_actually_declared():
    """Without this, cases 7 and 8 pass against contracts that declare nothing but
    column names."""
    entry = contracts.load("gl_entry")
    assert column(entry, "currency")["allowed"] == CURRENCIES
    for name in ("amount_dr", "amount_cr"):
        spec = column(entry, name)
        assert spec["min"] == 0 and spec["scale"] == 2
    assert column(entry, "version")["min"] == 1

    adjustment = contracts.load("gl_adjustment")
    assert set(column(adjustment, "adjustment_type")["allowed"]) == {"correction", "restatement"}
    # The adjustment table carries the same money and date rules as gl_entry. Only
    # asserting gl_entry's left these free to disappear silently.
    assert column(adjustment, "currency")["allowed"] == CURRENCIES
    for name in ("amount_dr", "amount_cr"):
        spec = column(adjustment, name)
        assert spec["min"] == 0 and spec["scale"] == 2
    assert column(adjustment, "version")["min"] == 1

    account = contracts.load("dim_account_src")
    assert sorted(column(account, "account_type")["allowed"]) == sorted(ACCOUNT_TYPES)

    rate = contracts.load("fx_rate")
    assert "min" in column(rate, "rate_to_base")
    assert column(rate, "currency")["allowed"] == CURRENCIES
    # No scale: a rate is not a currency amount, and pinning two places here would
    # make the contract demand a precision that is wrong for the domain.
    assert "scale" not in column(rate, "rate_to_base")

    # The columns, not just the type names: a not_after between the wrong two dates
    # would still satisfy a type-only assertion.
    for contract in (entry, adjustment):
        by_type = {c["type"]: c for c in contract.get("row_constraints", [])}
        assert {"not_after", "exactly_one_nonzero"} <= set(by_type), (
            f"{contract['table']} declares only {sorted(by_type)}"
        )
        assert by_type["not_after"]["earlier"] == "accounting_date"
        assert by_type["not_after"]["later"] == "posted_at"
        assert by_type["exactly_one_nonzero"]["columns"] == ["amount_dr", "amount_cr"]


def test_the_primary_keys_are_effective_dated_where_they_must_be():
    for table, expected in EXPECTED_PRIMARY_KEYS.items():
        assert contracts.load(table)["primary_key"] == expected

    # And no contract sneaks a single-column uniqueness rule back in for the
    # dimensions, which would reject a cost centre that legitimately moved.
    for table, code in (("dim_cost_center_src", "cc_code"), ("dim_account_src", "account_code")):
        contract = contracts.load(table)
        assert contract["primary_key"] != [code]
        for constraint in contract.get("row_constraints", []):
            assert constraint.get("columns") != [code]


# --- Cases 7-8: the rules hold against real data ---------------------------

def test_the_contracts_hold_for_clean_data(tmp_path):
    """Through the validator, which is what production does with these files. That
    covers the header classification and the primary-key check too, neither of which
    the rule executor this replaced could reach."""
    report = validate_source(generated(tmp_path))
    assert report.findings == [], report.describe()


def test_the_contracts_hold_with_every_business_switch_on(tmp_path):
    """The switches produce legitimate business events — a late entry, a restatement,
    a cost centre that changed department. A contract that called them violations
    would make the validator cry wolf in ordinary operation.

    schema_drift is excluded: adding or removing a column is what it does, and
    violating the column contract is the point of it.
    """
    out = generated(tmp_path, **{switch: True for switch in BUSINESS_SWITCHES})
    report = validate_source(out)
    assert report.findings == [], (
        f"a legitimate business scenario was reported as a violation:\n"
        f"{report.describe()}"
    )


# --- Cases 9-10: the boundary and the packaging ----------------------------

def test_the_loader_does_not_import_the_generator():
    """A contract derived from its producer cannot catch the producer changing."""
    # Parse the imports rather than grep for the word. The loader's docstring
    # explains why it does not import the generator, and a substring check cannot
    # tell that prose from an import statement.
    for name in ("ingest", "ingest.contracts", "ingest.validate"):
        module = importlib.import_module(name)
        tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
        offending = {n for n in imported if n == "eval.generator" or n.startswith("eval.generator.")}
        assert not offending, f"{name} imports {offending}"

    # In a fresh process, because this module imports the generator at the top: in
    # this interpreter it is already in sys.modules, so a before/after diff here can
    # never see the loader pulling it in. Verified by mutation - adding
    # `import generator` to the loader left the in-process check silent.
    program = (
        "import sys, importlib;"
        "importlib.import_module('ingest.contracts');"
        "print(sorted(n for n in sys.modules if n == 'eval.generator' or n.startswith('eval.generator.')))"
    )
    result = subprocess.run(
        [sys.executable, "-c", program],
        capture_output=True, text=True, check=True,
        cwd=Path(__file__).resolve().parent.parent,
    )
    assert result.stdout.strip() == "[]", (
        f"importing the loader pulled in {result.stdout.strip()}"
    )


# --- Added at stage 8 -------------------------------------------------------

MINIMAL = {
    "table": "t",
    "primary_key": ["a"],
    "columns": [{"name": "a", "type": "string", "nullable": False}],
    # Required since build-the-mart-layer: a contract has to say what it reaches
    # downstream, because dbt's graph cannot see the Spark hop. An empty list is a
    # statement. See docs/adr/0035.
    "feeds": [],
}


def validated(contract: dict):
    """Run the loader's own validation over a contract built in memory."""
    return contracts._validate(contract, "test.yaml")


def altered(**changes) -> dict:
    import copy
    contract = copy.deepcopy(MINIMAL)
    contract.update(changes)
    return contract


def test_a_contract_whose_table_disagrees_with_its_filename_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(contracts, "CONTRACT_DIR", tmp_path)
    contracts.load.cache_clear()
    (tmp_path / "gl_entry.yaml").write_text(
        "table: something_else\nprimary_key: [a]\nfeeds: []\n"
        "columns:\n  - name: a\n    type: string\n    nullable: false\n",
        encoding="utf-8",
    )
    with pytest.raises(contracts.ContractError, match="stale"):
        contracts.load("gl_entry")
    contracts.load.cache_clear()


@pytest.mark.parametrize("spec,value,expected", [
    ({"name": "d", "type": "date"}, "2026-1-5", "not a date"),
    ({"name": "d", "type": "date"}, "2026-13-01", "not a real date"),
    ({"name": "n", "type": "decimal"}, "NaN", "not a finite number"),
    ({"name": "n", "type": "decimal"}, "Infinity", "not a finite number"),
    ({"name": "n", "type": "decimal"}, "abc", "not a number"),
    ({"name": "n", "type": "decimal", "scale": 2}, "1.5", "decimal places"),
    ({"name": "n", "type": "decimal", "min": 0}, "-1.00", "below the minimum"),
    ({"name": "i", "type": "integer"}, "1.5", "not an integer"),
    ({"name": "s", "type": "string", "allowed": ["a"]}, "b", "not one of"),
])
def test_bad_values_are_reported_not_raised(spec, value, expected):
    """`2026-1-5` passes strptime and NaN raises on comparison rather than returning
    False — both would have slipped through or crashed the validator."""
    problem = check_value(spec, value)
    assert problem and expected in problem


# --- Added at stage 6 of merge-entries-idempotently -------------------------
#
# A contract now says which column its table advances its watermark on, and which
# column the raw layer partitions by. Cases 1-8 of task.md.

DATED = {
    "table": "t",
    "primary_key": ["a"],
    "columns": [
        {"name": "a", "type": "string", "nullable": False},
        {"name": "d", "type": "date", "nullable": False},
        {"name": "maybe", "type": "date", "nullable": True},
    ],
    # See MINIMAL above: required since build-the-mart-layer, docs/adr/0035.
    "feeds": [],
}


def dated(**changes) -> dict:
    import copy
    contract = copy.deepcopy(DATED)
    contract.update(changes)
    return contract


INCREMENTAL = {
    "gl_entry": ("posted_at", "accounting_date"),
    "gl_adjustment": ("posted_at", "accounting_date"),
}

@pytest.mark.parametrize("table,expected", sorted(INCREMENTAL.items()))
def test_the_entry_tables_declare_their_watermark_and_partition(table, expected):
    """Case 1. The source has no `updated_at`; `posted_at` is the column that already
    means when a row landed. See docs/adr/0014."""
    contract = contracts.load(table)
    watermark, partition_by = expected
    assert contract["watermark"] == watermark
    assert contract["partition_by"] == partition_by


@pytest.mark.parametrize("key", ["watermark", "partition_by"])
@pytest.mark.parametrize("value,expected", [
    # Case 3: names a column that does not exist.
    ("nope", "nope"),
    # Case 4: names a column that is not a date.
    ("a", "date"),
    # Case 5: names a column that may be null.
    ("maybe", "nullable"),
])
def test_a_watermark_that_cannot_be_compared_is_refused(key, value, expected):
    """Cases 3-5, and case 6 which is the same three for `partition_by`.

    A nullable watermark is the one that matters: it does not raise, it silently
    leaves a row out of every window it should have been in."""
    with pytest.raises(contracts.ContractError, match=expected):
        validated(dated(**{key: value}))


FULL_RELOAD = ("dim_account_src", "dim_cost_center_src", "dim_vendor", "fx_rate")


def test_the_date_keyed_sources_declare_immutability():
    """The contracts whose primary key carries the date a version took effect: the same
    key arriving changed is the source restating its own past rather than updating a
    row. See docs/adr/0023.

    `fx_rate` joined the list here rather than in the ticket that wrote the rule.
    docs/adr/0023 held it back deliberately - the shape and the argument were identical,
    but nothing consumed a rate yet, and deciding on behalf of a consumer that did not
    exist would have been guessing. The point-in-time join is that consumer.

    `dim_vendor` is keyed on the code alone with no date, so a supplier really does
    change its name in place. The entry tables carry `version` for the purpose this
    would serve, and whether their merge should refuse a changed row at an unchanged
    version is a different ticket's question."""
    for table in ("dim_account_src", "dim_cost_center_src", "fx_rate"):
        assert contracts.load(table).get("rows_are_immutable") is True, table

    for table in ("gl_entry", "gl_adjustment", "dim_vendor"):
        assert "rows_are_immutable" not in contracts.load(table), table


# --- the vendor columns and the vendor dimension -----------------------------
#
# Cases 26, 27 and 29a of task.md. See docs/adr/0022.

