# The lineage artefact is rendered from the manifest, not by dbt docs

summary: `transform/lineage.py` reads `manifest.json` and writes one self-contained
HTML file, because `dbt docs generate --static` does not embed the artefacts it
documents and dbt Docs v2 no longer produces a single file.

## Context

The requirement is a static HTML lineage graph as a build artefact — one file, opened
from disk, no server. Two facts about dbt decide how that is reached, and both were
checked against the documentation and the issue tracker on 2026-09-07 rather than
recalled.

`dbt docs generate --static` is documented to embed `manifest.json` and `catalog.json`
into `index.html`. It does not: dbt-labs/dbt-core#11986 reports the generated file
still fetching the external JSON, reproduced on dbt-core 1.10.8 and 1.9.4, open, with
no pull request and no milestone.

dbt Docs v2 does not offer a single file at all. The documented output is a
single-page app plus its artefacts under `target/`, or a `site/` directory with
`--output-dir`.

`manifest.json` itself is sound. It carries `parent_map` and `child_map`, schema v12,
written by every command that parses the project.

## Decision

`transform/lineage.py` reads `manifest.json` and writes `target/lineage.html`: one
file, the graph inlined as SVG, the node metadata inlined as JSON, no external request.
It is the same module `ingest/validate.py` walks the graph with, so the picture and the
impact list are read off one structure and cannot disagree.

`dbt docs generate` is still run in CI, because it is what refreshes `catalog.json` and
proves the project documents. Its `index.html` is not the artefact this project ships.

## Reasoning

Shipping dbt's multi-file site would technically satisfy "static", and it fails the
thing the requirement is for: a graph that answers what a column change breaks has to
open when someone double-clicks it, and dbt's `index.html` opened from a file path is a
blank page because the browser refuses the local JSON fetches.

Waiting for #11986 was declined. It has no assignee and no fix, and the artefact is one
of three acceptance criteria for this task.

Rendering it ourselves is small because the hard part is already done. The graph is
`child_map`; the layout is a topological ordering of six sources and a handful of
models; and at this size a hand-written SVG is more legible than a graph library would
be, with nothing to load from a CDN.

## Consequences

The rendered page is ours to maintain, and it will not gain features dbt's own docs
gain. That is the trade accepted: this artefact answers one question, and the question
is not "browse the project".
