"""The lineage graph: what a column change would break, and a picture of it.

Two callers, one module, so the picture and the impact list are read off one structure
and cannot disagree. `ingest/validate.py` asks what a failing source table reaches;
CI asks for an HTML file to keep as a build artefact.

The graph comes from dbt's `manifest.json`, which carries `parent_map` and `child_map`
and is written by any command that parses the project. The HTML does not come from
`dbt docs generate --static`: that flag does not embed the artefacts it documents
(dbt-labs/dbt-core#11986, open), and dbt Docs v2 has no single-file output at all. A
graph that answers what a column change breaks has to open when someone double-clicks
it, so it is rendered here. See docs/adr/0037.

    python -m transform.lineage --out transform/dbt/target/lineage.html
"""

import argparse
import json
from pathlib import Path

__all__ = ["MANIFEST", "ManifestMissing", "load_manifest", "source_names",
           "model_names", "downstream_of", "render", "main"]

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
NODE_WIDTH = 200
NODE_HEIGHT = 34
COLUMN_GAP = 120
ROW_GAP = 16
MARGIN = 30


def _layers(manifest: dict) -> list[list[str]]:
    """Nodes in columns: sources first, then each model after everything it reads."""
    parents = manifest.get("parent_map", {})
    interesting = [
        node for node in list(manifest.get("sources", {})) + list(manifest.get("nodes", {}))
        if node.startswith((SOURCE, MODEL))
    ]
    depth: dict[str, int] = {}

    def depth_of(node: str, guard: frozenset = frozenset()) -> int:
        if node in depth:
            return depth[node]
        if node in guard:
            return 0
        upstream = [p for p in parents.get(node, []) if p.startswith((SOURCE, MODEL))]
        value = 0 if not upstream else 1 + max(
            depth_of(p, guard | {node}) for p in upstream
        )
        depth[node] = value
        return value

    for node in interesting:
        depth_of(node)

    width = max(depth.values(), default=0) + 1
    columns: list[list[str]] = [[] for _ in range(width)]
    for node in sorted(interesting, key=_readable):
        columns[depth[node]].append(node)
    return columns


def _escape(text: str) -> str:
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace('"', "&quot;"))


def _svg(manifest: dict) -> tuple[str, int, int]:
    columns = _layers(manifest)
    placed: dict[str, tuple[int, int]] = {}
    for index, column in enumerate(columns):
        for row, node in enumerate(column):
            placed[node] = (
                MARGIN + index * (NODE_WIDTH + COLUMN_GAP),
                MARGIN + row * (NODE_HEIGHT + ROW_GAP),
            )

    width = MARGIN * 2 + len(columns) * NODE_WIDTH + max(len(columns) - 1, 0) * COLUMN_GAP
    height = MARGIN * 2 + max((len(c) for c in columns), default=1) * (NODE_HEIGHT + ROW_GAP)

    edges = []
    for parent, children in sorted(manifest.get("child_map", {}).items()):
        if parent not in placed:
            continue
        for child in sorted(children):
            if child not in placed:
                continue
            x1, y1 = placed[parent]
            x2, y2 = placed[child]
            edges.append(
                f'<line x1="{x1 + NODE_WIDTH}" y1="{y1 + NODE_HEIGHT // 2}" '
                f'x2="{x2}" y2="{y2 + NODE_HEIGHT // 2}" class="edge" />'
            )

    boxes = []
    for node, (x, y) in sorted(placed.items(), key=lambda item: _readable(item[0])):
        kind = "source" if node.startswith(SOURCE) else "model"
        label = _escape(_readable(node))
        boxes.append(
            f'<g class="node {kind}">'
            f'<rect x="{x}" y="{y}" width="{NODE_WIDTH}" height="{NODE_HEIGHT}" rx="5" />'
            f'<text x="{x + NODE_WIDTH // 2}" y="{y + NODE_HEIGHT // 2 + 4}">{label}</text>'
            f"</g>"
        )

    body = "\n".join(edges + boxes)
    return body, width, height


TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>fin-pipeline lineage</title>
<style>
body {{ font: 14px/1.5 system-ui, sans-serif; margin: 2rem; color: #1a1a1a; }}
h1 {{ font-size: 1.1rem; }}
p {{ max-width: 60ch; color: #444; }}
svg {{ border: 1px solid #ddd; background: #fff; max-width: 100%; }}
.edge {{ stroke: #b0b0b0; stroke-width: 1.5; }}
.node rect {{ fill: #f4f6f8; stroke: #8a94a0; }}
.node.source rect {{ fill: #eef4ea; stroke: #7a9a68; }}
.node text {{ text-anchor: middle; font: 12px monospace; fill: #1a1a1a; }}
</style>
</head>
<body>
<h1>fin-pipeline lineage</h1>
<p>Sources on the left, models to the right of everything they read. This answers one
question — what a column change would break — and it is rendered from
<code>manifest.json</code> so it opens from a file path with nothing to fetch.</p>
<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}"
     xmlns="http://www.w3.org/2000/svg">
{body}
</svg>
</body>
</html>
"""


def render(manifest: dict, out) -> Path:
    """Write the graph as one self-contained file.

    Deterministic: everything is sorted, and nothing carries a timestamp. A build
    artefact that differs every time it is produced cannot be diffed, and one nobody
    can diff is one nobody reads.
    """
    body, width, height = _svg(manifest)
    target = Path(out)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        TEMPLATE.format(body=body, width=width, height=height), encoding="utf-8"
    )
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="transform.lineage", description=__doc__)
    parser.add_argument("--manifest", default=None)
    parser.add_argument("--out", default=str(PROJECT / "target" / "lineage.html"))
    args = parser.parse_args(argv)

    target = render(load_manifest(args.manifest), args.out)
    print(f"lineage -> {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
