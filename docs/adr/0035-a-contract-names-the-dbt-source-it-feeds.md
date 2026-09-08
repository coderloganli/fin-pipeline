# A contract names the dbt source it feeds

summary: Each `ingest/contracts/*.yaml` declares a `feeds` list of dbt source names, so
contract validation can walk the manifest's `child_map` and name the models a column
change would break.

## Context

`docs/adr/0012` left `ingest/validate.py` reporting "downstream impact: unknown" when a
contract fails, because no lineage graph existed. The first acceptance criterion that
went unmet in the first phase was exactly this: a dropped column should name the
downstream models it would break.

dbt now supplies a graph. It does not supply this one. dbt's sources are the Postgres
tables `transform/load.py` writes; the hop from `gl_entry.csv` to
`staging.fct_gl_entry` happens inside `transform/spark/facts.py` and appears in no
manifest. A manifest walk alone can say what breaks below the staging tables and
cannot say which staging table a given CSV feeds.

## Decision

Every contract declares the dbt sources it feeds:

```yaml
table: gl_entry
feeds: [staging.fct_gl_entry]
```

`feeds` joins `TOP_LEVEL` in `ingest/contracts/__init__.py`, which is a strict allowlist
— an unrecognised top-level key fails the contract, so this is a change to the contract
schema and not just to the files. It is required rather than optional: an optional key
would let a new contract silently feed nothing, and `feeds: []` already says that out
loud for the one table it is true of.

The source names are qualified with the schema they are declared under —
`landing.fct_gl_entry` — matching what the dbt project declares. See docs/adr/0034 for
why that schema is `landing` and not `staging`.

`transform/lineage.py` reads `manifest.json`, resolves those names to source nodes, and
walks `child_map` transitively. `ingest/validate.py` calls it from inside
`downstream_impact()` and lists what it returns.

`gl_adjustment` declares `feeds: []`, because nothing consumes it yet. An empty list is
a statement, and validation says so — "no model consumes this table yet" — rather than
printing an empty section.

Where the manifest has not been built, validation says the manifest is missing and
names the command that builds it. It does not fall back to silence.

The import of `transform.lineage` inside `ingest.validate` is deliberate and it is
lazy. It is an edge from ingest to transform, which is the wrong direction, and it is
accepted because the alternative is worse: a second manifest reader inside ingest, or a
copy of the graph in a format ingest invents. Keeping it lazy means `ingest` still
imports with no dbt project on disk, which is what the ingest tests rely on.

## Reasoning

The Spark hop is real and no amount of configuration makes dbt see it. The two honest
options were to declare it or to admit the graph stops at the staging tables. Declaring
it costs one line per contract and makes the third acceptance criterion reachable;
admitting the gap makes the criterion half-met forever.

Putting the declaration in the contract rather than in a separate mapping file is what
keeps it from drifting. The contract is already the single statement of what ingest
expects from a source table — `docs/adr/0008` — and "what depends on this table" is the
same kind of fact as "what columns this table has". A mapping kept elsewhere would be
edited by whoever noticed it was stale, which is nobody.

Rewriting `transform/spark/` as dbt models would have given native end-to-end lineage.
It was declined as a rewrite of two landed modules to buy a property one declaration
already buys.

## Consequences

The declaration can go stale: a contract could name a source that a later refactor
renames. A test asserts every `feeds` entry resolves to a source that exists in the
manifest, so a rename breaks the suite rather than quietly emptying the impact list.
