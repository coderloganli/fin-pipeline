"""The lineage graph: what a column change would break.

The graph comes from dbt's `manifest.json`, which carries `parent_map` and `child_map`
and is written by any command that parses the project. `ingest/validate.py` is the
caller: when a source table fails its contract, the failure names the models downstream
of it rather than saying the impact is unknown.

Every edge in that graph is a `ref()`, because every transformation is a model. The one
hop that is not - the copy `transform/load.py` performs from the raw layer into Postgres
- is why each contract declares the dbt source it feeds. See docs/adr/0035.
"""

import json
from pathlib import Path

__all__ = ["ManifestMissing", "load_manifest", "source_names",
           "model_names", "downstream_of", "unresolved"]

PROJECT = Path(__file__).resolve().parent / "dbt"
MANIFEST = PROJECT / "target" / "manifest.json"

BUILD_COMMAND = "dbt build --project-dir transform/dbt --profiles-dir transform/dbt"

# The prefixes dbt gives its node ids. A source is `source.<project>.<source>.<table>`
# and a model is `model.<project>.<name>`.
SOURCE = "source."
MODEL = "model."


class ManifestMissing(FileNotFoundError):
    """No manifest on disk.

    Said rather than returned empty: an empty impact section would let the unfinished
    half of a requirement pass for finished, which is the reason docs/adr/0012 gave for
    stating the gap rather than omitting it.
    """


def load_manifest(path=None) -> dict:
    path = Path(path) if path is not None else MANIFEST
    if not path.is_file():
        raise ManifestMissing(
            f"no dbt manifest at {path}. It is written by any command that parses the "
            f"project - run `{BUILD_COMMAND}`, or `dbt parse` for the manifest alone."
        )
    return json.loads(path.read_text(encoding="utf-8"))


def _readable(node_id: str) -> str:
    """A node id as the name a person wrote.

    `source.fin_pipeline.landing.fct_gl_entry` is `landing.fct_gl_entry`, which is what
    a contract's `feeds` declares. `model.fin_pipeline.fct_gl_entry` is
    `mart.fct_gl_entry`, qualified so a model and the source it reads are never the
    same string.
    """
    parts = node_id.split(".")
    if node_id.startswith(SOURCE):
        return ".".join(parts[2:])
    if node_id.startswith(MODEL):
        return f"mart.{parts[-1]}"
    return node_id


def source_names(manifest: dict) -> set[str]:
    return {_readable(node) for node in manifest.get("sources", {})}


def model_names(manifest: dict) -> set[str]:
    return {
        _readable(node)
        for node, entry in manifest.get("nodes", {}).items()
        if entry.get("resource_type") == "model"
    }


def _source_ids(manifest: dict, names) -> dict[str, str]:
    """Declared name to node id, for the names that resolve."""
    wanted = set(names)
    return {
        _readable(node): node
        for node in manifest.get("sources", {})
        if _readable(node) in wanted
    }


def downstream_of(names, manifest=None, path=None) -> list[str]:
    """Every model reachable from these sources, transitively.

    Transitively, because a one-level walk stops before the models that read the models
    - and the question being asked is what would break, not what would break first.
    """
    manifest = manifest if manifest is not None else load_manifest(path)
    children = manifest.get("child_map", {})
    resolved = _source_ids(manifest, names)

    seen: set[str] = set()
    frontier = list(resolved.values())
    while frontier:
        node = frontier.pop()
        for child in children.get(node, []):
            if child in seen:
                continue
            seen.add(child)
            frontier.append(child)

    return sorted(_readable(node) for node in seen if node.startswith(MODEL))


def unresolved(names, manifest=None, path=None) -> list[str]:
    """The declared names the manifest does not know. A rename shows up here."""
    manifest = manifest if manifest is not None else load_manifest(path)
    return sorted(set(names) - set(_source_ids(manifest, names)))


# --- the picture -----------------------------------------------------------

# Laid out by hand rather than by a library: at six sources and seven models a
# topological ordering into columns is more legible than a force-directed graph, and it
# loads nothing from a CDN - which is the requirement, not a preference.
