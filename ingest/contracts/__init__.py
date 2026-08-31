"""Loading and validating the source-table contracts.

A contract states what ingest expects one source table to look like: its columns and
their order, each column's type and nullability, permitted values or bounds where
those exist, the primary key, and any rule that spans columns rather than qualifying
one.

This module loads those files and checks that the *contract itself* is well formed.
**It does not look at data.** Applying a contract to rows — and deciding that an added
column is compatible while a dropped one is not — belongs to `ingest.validate`, which
now exists and owns the rule primitives. They did not move into this package: it would
have made the paragraph above false and left one module doing two unrelated jobs. What
stays here is what a contract *means* rather than how to check it — `is_null` and
`DATE_FORMAT` — and the validator imports both from here rather than restating them.
See docs/adr/0010-the-validator-owns-the-rule-primitives.md.

Nothing here imports the generator. A contract derived from its producer cannot catch
the producer changing; two independently written statements can disagree, and that
disagreement is the whole product. See
docs/adr/0008-contracts-are-written-by-hand.md.
"""

import re
from functools import cache
from pathlib import Path

import yaml

CONTRACT_DIR = Path(__file__).resolve().parent

# The format a `date` column is written in. The loader states it; applying it to data
# is not this module's job.
DATE_FORMAT = "%Y-%m-%d"

TYPES = frozenset({"string", "integer", "decimal", "date"})

# Which modifiers each type accepts. `scale` on a string is meaningless, and a
# contract that says so is a contract nobody can trust the rest of.
MODIFIERS: dict[str, frozenset[str]] = {
    "string": frozenset({"allowed"}),
    "integer": frozenset({"min"}),
    "decimal": frozenset({"min", "scale"}),
    "date": frozenset(),
}

ROW_CONSTRAINTS = frozenset({"not_after", "exactly_one_nonzero"})

REQUIRED_CONSTRAINT_KEYS: dict[str, frozenset[str]] = {
    "not_after": frozenset({"earlier", "later"}),
    "exactly_one_nonzero": frozenset({"columns"}),
}

TOP_LEVEL = frozenset({
    "table", "primary_key", "columns", "row_constraints", "watermark", "partition_by",
})
REQUIRED = frozenset({"table", "primary_key", "columns"})

# Both name a column the load reads as a date and must be able to compare: `watermark`
# is what the incremental load advances on, `partition_by` is what the raw layer
# partitions by. Absence is meaningful - a table that declares no watermark is loaded
# in full. See docs/adr/0014-the-contract-declares-the-watermark-column.md.
DATE_KEYS = ("watermark", "partition_by")


class ContractError(ValueError):
    """The contract file itself is wrong — not the data it describes."""


def is_null(value: str) -> bool:
    """An empty field is the null value.

    Said rather than assumed: without it a validator could treat `""` as a perfectly
    good string, and every `nullable: false` in every contract would mean nothing.
    """
    return value == ""


def _validate(contract: dict, source: str) -> dict:
    def fail(message: str) -> None:
        raise ContractError(f"{source}: {message}")

    if not isinstance(contract, dict):
        fail(f"a contract must be a mapping, got {type(contract).__name__}")
    unknown = set(contract) - TOP_LEVEL
    if unknown:
        fail(f"unknown top-level keys {sorted(unknown)}")
    missing = REQUIRED - set(contract)
    if missing:
        fail(f"missing {sorted(missing)}")
    if not isinstance(contract["table"], str) or not contract["table"]:
        fail(f"table must be a name, got {contract['table']!r}")

    if not isinstance(contract["columns"], list) or not contract["columns"]:
        fail("columns must be a non-empty list")

    names = []
    for spec in contract["columns"]:
        if not isinstance(spec, dict):
            fail(f"a column entry is {type(spec).__name__}, not a mapping")
        name = spec.get("name")
        if not isinstance(name, str) or not name:
            fail("a column has no name")
        if name in names:
            fail(f"column {name!r} appears twice")
        names.append(name)

        if spec.get("type") not in TYPES:
            fail(f"{name}: type must be one of {sorted(TYPES)}, got {spec.get('type')!r}")
        # Required rather than defaulted: a contract that leaves nullability
        # unstated is a contract nobody can act on.
        if "nullable" not in spec:
            fail(f"{name}: nullable must be stated")
        if not isinstance(spec["nullable"], bool):
            fail(f"{name}: nullable must be true or false")
        if "allowed" in spec:
            permitted_values = spec["allowed"]
            if not isinstance(permitted_values, list) or not permitted_values:
                fail(f"{name}: allowed must be a non-empty list")
            # Strings, because a CSV field is a string. `allowed: [1, 2]` would load
            # happily and then reject the perfectly good value "1".
            if not all(isinstance(item, str) for item in permitted_values):
                fail(f"{name}: allowed must list strings, got {permitted_values}")
        # bool is a subclass of int, so `min: false` would otherwise pass as 0.
        if "min" in spec and (
            isinstance(spec["min"], bool) or not isinstance(spec["min"], (int, float))
        ):
            fail(f"{name}: min must be a number, got {spec['min']!r}")
        if "scale" in spec:
            if not isinstance(spec["scale"], int) or isinstance(spec["scale"], bool) or spec["scale"] < 0:
                fail(f"{name}: scale must be a non-negative whole number, got {spec['scale']!r}")

        permitted = MODIFIERS[spec["type"]] | {"name", "type", "nullable"}
        for key in spec:
            if key not in permitted:
                fail(f"{name}: {spec['type']} columns cannot take {key!r}")

    for key in DATE_KEYS:
        if key not in contract:
            continue
        named = contract[key]
        if not isinstance(named, str) or not named:
            fail(f"{key} must be a column name, got {named!r}")
        spec = next((entry for entry in contract["columns"] if entry["name"] == named), None)
        if spec is None:
            fail(f"{key} names {named!r}, which is not a column")
        if spec["type"] != "date":
            fail(f"{key} names {named!r}, which is a {spec['type']} rather than a date")
        # A watermark that can be absent cannot be compared, and the failure is not an
        # error: the row is silently left out of every window it belonged in.
        if spec["nullable"]:
            fail(f"{key} names {named!r}, which is nullable")

    primary_key = contract["primary_key"]
    if not isinstance(primary_key, list) or not primary_key:
        fail("primary_key must be a non-empty list")
    if not all(isinstance(key, str) and key for key in primary_key):
        fail(f"primary_key must be column names: {primary_key}")
    if len(primary_key) != len(set(primary_key)):
        fail(f"primary_key repeats a column: {primary_key}")
    for key in primary_key:
        if key not in names:
            fail(f"primary_key names {key!r}, which is not a column")

    row_constraints = contract.get("row_constraints", [])
    if not isinstance(row_constraints, list):
        fail("row_constraints must be a list")
    for constraint in row_constraints:
        if not isinstance(constraint, dict):
            fail(f"a row constraint is {type(constraint).__name__}, not a mapping")
        kind = constraint.get("type")
        if kind not in ROW_CONSTRAINTS:
            fail(f"unknown row constraint {kind!r}")

        # Each type has exactly the keys it needs. Without this, an
        # exactly_one_nonzero that forgot its columns defaults to [] and passes,
        # silently checking nothing.
        expected = REQUIRED_CONSTRAINT_KEYS[kind]
        if set(constraint) != expected | {"type"}:
            fail(f"{kind} takes exactly {sorted(expected)}, got {sorted(set(constraint) - {'type'})}")

        if kind == "exactly_one_nonzero":
            referenced = constraint["columns"]
            if not isinstance(referenced, list) or len(referenced) < 2:
                fail("exactly_one_nonzero needs at least two columns")
            # Names before set(): an unhashable member would raise TypeError out of
            # the loader instead of being reported as the contract error it is.
            if not all(isinstance(item, str) for item in referenced):
                fail(f"exactly_one_nonzero must list column names: {referenced}")
            if len(referenced) != len(set(referenced)):
                fail(f"exactly_one_nonzero repeats a column: {referenced}")
        else:
            referenced = [constraint["earlier"], constraint["later"]]
        for name in referenced:
            if name not in names:
                fail(f"row constraint {kind} names {name!r}, which is not a column")

    return contract


@cache
def load(table: str) -> dict:
    """Read and validate one table's contract."""
    path = CONTRACT_DIR / f"{table}.yaml"
    if not path.is_file():
        raise ContractError(f"no contract for {table!r} at {path}")
    contract = _validate(yaml.safe_load(path.read_text(encoding="utf-8")), path.name)
    if contract["table"] != table:
        raise ContractError(
            f"{path.name}: declares table {contract['table']!r}, but the file is named "
            f"{table!r}; a mismatch here means one of them is stale"
        )
    return contract


def tables() -> list[str]:
    return sorted(path.stem for path in CONTRACT_DIR.glob("*.yaml"))
