"""The lineage graph, and the debt it pays off.

docs/adr/0012 left contract validation reporting "downstream impact: unknown" because
no lineage graph existed. It exists now, and this file is what says the unmet
acceptance criterion from the first phase is met: a dropped column fails the run and
names the models that would have broken.

dbt's graph does not reach the CSV source tables - the hop from `gl_entry.csv` to
`landing.fct_gl_entry` happens inside `transform/spark/facts.py` and appears in no
manifest - so each contract declares which dbt source it feeds. See docs/adr/0035.

The HTML artefact is rendered from the manifest rather than by `dbt docs generate
--static`, which does not embed the artefacts it documents. See docs/adr/0037.

Cases 60-71 of task.md.
"""

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from ingest import contracts

CONTRACT_DIR = Path(contracts.__file__).parent
ALL_TABLES = sorted(path.stem for path in CONTRACT_DIR.glob("*.yaml"))

# What the manifest calls a source node, and what a contract's `feeds` entry looks like.
SOURCE_PATTERN = re.compile(r"^[a-z_]+\.[a-z_]+$")


def manifest_path() -> Path:
    from conftest import DBT_PROJECT

    return DBT_PROJECT / "target" / "manifest.json"


@pytest.fixture(scope="module")
def manifest(dbt_manifest):
    """The parsed manifest. `dbt_manifest` builds it if the working tree has none."""
    return json.loads(Path(dbt_manifest).read_text(encoding="utf-8"))


def contract_yaml(table: str) -> dict:
    return yaml.safe_load((CONTRACT_DIR / f"{table}.yaml").read_text(encoding="utf-8"))


# --- cases 60-62: the declaration ------------------------------------------

def test_feeds_is_required_of_every_contract():
    """60. Required, not optional. An optional key would let a new contract silently
    feed nothing, and `feeds: []` already says that out loud for the one table it is
    true of."""
    for table in ALL_TABLES:
        assert "feeds" in contract_yaml(table), f"{table} declares no feeds"

    incomplete = dict(contract_yaml("gl_entry"))
    incomplete.pop("feeds")

    with pytest.raises(contracts.ContractError) as failure:
        contracts._validate(incomplete, "no_feeds.yaml")
    assert "feeds" in str(failure.value)


def test_feeds_must_be_qualified_source_names():
    """61. A list of `schema.table` strings. A bare table name would resolve against
    nothing and empty the impact list without anything noticing."""
    for table in ALL_TABLES:
        feeds = contract_yaml(table)["feeds"]
        assert isinstance(feeds, list)
        for name in feeds:
            assert SOURCE_PATTERN.fullmatch(name), f"{table} feeds {name!r}"

    broken = dict(contract_yaml("gl_entry"))
    broken["feeds"] = ["fct_gl_entry"]

    with pytest.raises(contracts.ContractError) as failure:
        contracts._validate(broken, "bare_name.yaml")
    assert "fct_gl_entry" in str(failure.value)


def test_every_declared_source_exists_in_the_manifest(manifest):
    """62. A renamed source breaks this test rather than quietly emptying the impact
    list. The declaration can go stale; this is what stops it going stale in silence."""
    from transform import lineage

    known = lineage.source_names(manifest)
    for table in ALL_TABLES:
        for name in contract_yaml(table)["feeds"]:
            assert name in known, f"{table} feeds {name!r}, which the manifest has not"


# --- cases 63-64: the walk -------------------------------------------------

def test_downstream_of_a_source_names_its_models(manifest):
    """63. And not models that descend only from other sources. An impact list that
    named everything would be as useless as one that named nothing."""
    from transform import lineage

    affected = lineage.downstream_of(["landing.fct_gl_entry"], manifest=manifest)

    assert "mart.fct_gl_entry" in affected
    assert "mart.dim_vendor" not in affected


def test_the_walk_is_transitive(manifest):
    """64. Not one level. `model_row_count` depends on the models, which depend on the
    sources, and a one-level walk would stop before it."""
    from transform import lineage

    affected = lineage.downstream_of(["landing.fct_gl_entry"], manifest=manifest)
    direct = set(manifest["child_map"].get("source.fin_pipeline.landing.fct_gl_entry", []))

    assert len(affected) > len(direct)
    assert "mart.model_row_count" in affected


# --- cases 65-68: what validation says now ---------------------------------

def test_a_dropped_column_names_the_downstream_models(tmp_path, dbt_manifest):
    """65. The acceptance criterion from the first phase that went unmet. docs/adr/0012
    said the downstream impact was unknown because there was no graph to ask."""
    from generator import generate
    from generator.config import Config

    source = tmp_path / "source"
    generate(Config(seed=42, out_dir=source, periods="2026-01:2026-01",
                    entries_per_period=20, schema_drift="drop_column",
                    schema_drift_table="gl_entry"))

    result = subprocess.run(
        [sys.executable, "-m", "ingest.validate", "--source", str(source)],
        capture_output=True, text=True,
    )

    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert "mart.fct_gl_entry" in output

    # The models the failing table reaches, not every model in the project. An impact
    # list that names everything is as useless as one that names nothing, and the CLI
    # used to hand `downstream_impact` every table it had checked - which went unnoticed
    # while the function ignored its argument.
    assert "mart.dim_vendor" not in output


def test_a_table_that_feeds_nothing_says_so(manifest):
    """66. `gl_adjustment` is consumed by nothing yet. An empty list is a statement,
    and reporting it as an unknown would be reporting a fact as an absence."""
    from ingest import validate

    assert contract_yaml("gl_adjustment")["feeds"] == []
    message = validate.downstream_impact(["gl_adjustment"])

    assert "no model consumes" in message.lower()
    assert "unknown" not in message.lower()


def test_a_missing_manifest_says_so_and_names_the_command(tmp_path, monkeypatch):
    """67. It does not silently return nothing. An empty impact section would let the
    unfinished half of the requirement pass for finished - which is the reason
    docs/adr/0012 gave for stating the gap rather than omitting it."""
    from ingest import validate
    from transform import lineage

    monkeypatch.setattr(lineage, "MANIFEST", tmp_path / "absent" / "manifest.json")
    message = validate.downstream_impact(["gl_entry"])

    assert "manifest" in message.lower()
    assert "dbt" in message.lower()


def test_ingest_imports_without_a_dbt_project():
    """68. The `transform.lineage` import is lazy, and validation still answers.

    `ingest` has to import with no dbt project on disk - the ingest tests rely on it,
    and an ingest run is not a dbt run. Two halves: the import is not taken at module
    level, and `downstream_impact` still returns something, rather than the fixed
    "no lineage graph exists yet" string docs/adr/0012 left as a placeholder."""
    probe = (
        "import sys, ingest.validate as v;"
        "assert 'transform.lineage' not in sys.modules,"
        " 'ingest.validate imported transform.lineage at module level';"
        "message = v.downstream_impact(['gl_entry']);"
        "assert 'No lineage graph exists yet' not in message, message;"
        "print(message)"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr


# --- cases 69-71: the artefact ---------------------------------------------

def test_the_rendered_page_needs_nothing_from_the_network(manifest, tmp_path):
    """69. Opening it from a file path is the requirement it exists for. dbt's own
    index.html opened that way is a blank page, because the browser refuses the local
    JSON fetches - which is the whole reason this is rendered here."""
    from transform import lineage

    target = tmp_path / "lineage.html"
    lineage.render(manifest, target)
    html = target.read_text(encoding="utf-8")

    # Not just absolute URLs. A relative `src="assets/app.js"` is equally fatal when
    # the file is opened from a path, and it is what dbt's own index.html does.
    assert not re.search(r"<(script|link|img|iframe|object|embed)\b", html, re.I)
    assert not re.search(r'\b(src|href)\s*=', html, re.I)
    assert "fetch(" not in html
    assert "XMLHttpRequest" not in html
    assert "url(" not in html


def test_the_rendered_page_holds_the_whole_graph(manifest, tmp_path):
    """70. Every source, every model, and an edge for every pair in `child_map`. A
    graph missing an edge answers the question it exists for with a wrong answer."""
    from transform import lineage

    target = tmp_path / "lineage.html"
    lineage.render(manifest, target)
    html = target.read_text(encoding="utf-8")

    for name in lineage.source_names(manifest) | lineage.model_names(manifest):
        assert name in html, f"{name} is missing from the rendered graph"

    # Between sources and models only. `child_map` counts dbt's test nodes as children
    # of the models they test, and the graph draws the same set `downstream_of`
    # returns - drawing the gates would take it from thirteen nodes to over thirty and
    # put the picture at odds with the impact list it exists to agree with.
    drawn = {
        node for node in list(manifest["sources"]) + list(manifest["nodes"])
        if node.startswith(("source.", "model."))
    }
    edges = sum(
        len([child for child in children if child in drawn])
        for parent, children in manifest["child_map"].items()
        if parent in drawn
    )
    # Exactly, not at least. A graph with spurious edges answers the question it exists
    # for with a wrong answer just as surely as one missing them.
    assert html.count("<line") == edges


def test_a_cycle_in_the_manifest_does_not_hang_the_layout(tmp_path):
    """70a. dbt will not produce one, and this module should not be the thing that
    discovers it by recursing until the stack ends. A guard that is never exercised is
    a guard nobody knows the shape of."""
    from transform import lineage

    cyclic = {
        "sources": {},
        "nodes": {
            "model.p.a": {"resource_type": "model"},
            "model.p.b": {"resource_type": "model"},
        },
        "parent_map": {"model.p.a": ["model.p.b"], "model.p.b": ["model.p.a"]},
        "child_map": {"model.p.a": ["model.p.b"], "model.p.b": ["model.p.a"]},
    }

    target = tmp_path / "cyclic.html"
    lineage.render(cyclic, target)
    html = target.read_text(encoding="utf-8")

    assert "mart.a" in html and "mart.b" in html
    assert lineage.downstream_of(["anything"], manifest=cyclic) == []


def test_a_renamed_source_is_reported_rather_than_dropped(manifest):
    """62a. `unresolved` is what turns a stale declaration into a message. Without it
    the impact list for a renamed source is empty, which reads exactly like a table
    nothing depends on."""
    from transform import lineage

    assert lineage.unresolved(["landing.fct_gl_entry"], manifest=manifest) == []
    assert lineage.unresolved(
        ["landing.gone_away", "landing.fct_gl_entry"], manifest=manifest
    ) == ["landing.gone_away"]


def test_a_table_that_feeds_something_and_one_that_does_not(manifest):
    """66a. Asked about both at once, the message has to name the models rather than
    fall back to the sentence for a table that feeds nothing."""
    from ingest import validate

    message = validate.downstream_impact(["gl_entry", "gl_adjustment"])

    assert "mart.fct_gl_entry" in message
    assert "no model consumes" not in message.lower()


def test_re_rendering_an_unchanged_manifest_is_byte_identical(manifest, tmp_path):
    """71. The artefact does not churn between builds. A file that differs every time
    it is produced cannot be diffed, and a build artefact nobody can diff is a build
    artefact nobody reads."""
    from transform import lineage

    first, second = tmp_path / "one.html", tmp_path / "two.html"
    lineage.render(manifest, first)
    lineage.render(manifest, second)

    assert first.read_bytes() == second.read_bytes()
