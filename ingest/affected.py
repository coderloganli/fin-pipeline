"""Which accounting periods a run dirtied, and which dimension versions it landed.

Both merge paths already know. `ingest.load.load_table` groups its batch by accounting
period and merges only the periods it touches, and `ingest.raw.merge_table` counts an
update only when a declared column actually differs - so a nightly extract re-presenting
history it has already landed reports nothing. Both used to keep only a count. This
module is where the identity survives instead.

Two triggers, not one. An entry landing in a period dirties it, and a dimension version
taking effect over a period dirties it too. All three effective-dated tables declare
`rows_are_immutable`, so a dimension change can only ever be the insert of a new
`(natural key, effective_date)` version - which makes that trigger exact rather than a
comparison of attributes.

Ingest records observations. Turning a dimension version into a set of periods needs the
version's validity interval and the periods that carry entries on that key, and both of
those belong to `transform/`. See `transform/spark/affected.py`.

    python -m ingest.affected [--raw data/raw] [--clear]

See docs/adr/0039.
"""

import argparse
import json
import os
from dataclasses import dataclass, field
from pathlib import Path

__all__ = ["STATE_FILE", "Affected", "path_for", "read", "record", "clear", "main"]

STATE_DIR = "_state"
STATE_FILE = "affected_periods.json"


class StateError(ValueError):
    """The state file holds something that cannot be used. Checked on the way in,
    before anything has been written, for the reason `Watermarks.load` checks its
    own: a shape that cannot be used otherwise raises much later, halfway through a
    run and a long way from the file that caused it."""


@dataclass
class Affected:
    """What is owed. Periods to recompute, and versions still to be interpreted."""

    periods: list[str] = field(default_factory=list)
    dimension_versions: list[dict] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.periods and not self.dimension_versions

    def describe(self) -> str:
        if self.is_empty():
            return "nothing owed"
        lines = []
        if self.periods:
            lines.append(f"periods: {', '.join(self.periods)}")
        for version in self.dimension_versions:
            key = ", ".join(version["key"])
            lines.append(
                f"version: {version['table']} ({key}) effective {version['effective_date']}"
            )
        return "\n".join(lines)


def path_for(raw_dir) -> Path:
    return Path(raw_dir) / STATE_DIR / STATE_FILE


def version_key(version: dict) -> tuple:
    return (version["table"], tuple(version["key"]), version["effective_date"])


def read(raw_dir) -> Affected:
    """What the file holds. A missing file is an empty set, not an error: a fresh raw
    layer owes nothing, and raising there would make the first run of a clone fail."""
    path = path_for(raw_dir)
    if not path.is_file():
        return Affected()

    values = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(values, dict):
        raise StateError(
            f"{path}: expected an object with `periods` and `dimension_versions`, "
            f"found {type(values).__name__}. Delete the file to recompute everything."
        )
    periods = values.get("periods", [])
    versions = values.get("dimension_versions", [])
    if not isinstance(periods, list) or not all(isinstance(p, str) for p in periods):
        raise StateError(f"{path}: `periods` is not a list of period strings")
    if not isinstance(versions, list):
        raise StateError(f"{path}: `dimension_versions` is not a list")
    for version in versions:
        if not isinstance(version, dict) or not {"table", "key", "effective_date"} <= set(version):
            raise StateError(
                f"{path}: a dimension version must carry `table`, `key` and "
                f"`effective_date`, found {version!r}"
            )
    return Affected(periods=sorted(set(periods)),
                    dimension_versions=list(versions))


def record(raw_dir, *, periods=(), versions=()) -> Affected:
    """Union what is given into what is held, and write it.

    Union rather than replace, because a transform may not have run since the last
    ingest and a set that forgot the run before it would leave those periods stale for
    good. Written the way a partition is written - to a temporary file, then moved -
    for the reason `Watermarks.save` is: a half-written state file is worse than a
    missing one.
    """
    held = read(raw_dir)
    merged = Affected(
        periods=sorted(set(held.periods) | {p for p in periods if p}),
        dimension_versions=list(held.dimension_versions),
    )
    seen = {version_key(v) for v in merged.dimension_versions}
    for version in versions:
        normalised = {"table": version["table"], "key": list(version["key"]),
                      "effective_date": version["effective_date"]}
        if version_key(normalised) not in seen:
            seen.add(version_key(normalised))
            merged.dimension_versions.append(normalised)
    merged.dimension_versions.sort(key=version_key)

    _write(raw_dir, merged)
    return merged


def clear(raw_dir) -> None:
    """Nothing is owed. The orchestrator's step, not something a consumer does on its
    own: a module that silently cleared shared state on success cannot be run twice, or
    alone, without consequences that are not visible where the command is typed."""
    _write(raw_dir, Affected())


def _write(raw_dir, state: Affected) -> None:
    path = path_for(raw_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        temporary.write_text(
            json.dumps(
                {"periods": state.periods,
                 "dimension_versions": state.dimension_versions},
                indent=2, sort_keys=True,
            ) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ingest.affected", description=__doc__)
    parser.add_argument("--raw", default="data/raw", help="the raw layer")
    parser.add_argument("--clear", action="store_true",
                        help="mark everything as recomputed")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.clear:
        clear(args.raw)
        print("cleared")
        return 0
    print(read(args.raw).describe())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
