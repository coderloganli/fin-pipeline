# Architecture

<!-- Keep this under about 350 lines. Every task reads it in full, and a document
     too long to read in one sitting stops being read. Detail that belongs to one
     decision goes in docs/adr/ instead. -->

## What this is

A finance data platform. General-ledger entries land unchanged, are modelled in
layers, are attributed against the account hierarchy and exchange rate that were in
effect on the transaction date, and are served as query-ready tables. An anomaly
model flags balances that fall outside its prediction interval, and an LLM layer
investigates each flagged balance and writes an explanation that cites the source
entries it relied on. The hard problems here are time semantics — point-in-time
correctness, slowly changing dimensions, late-arriving corrections, idempotent
replay — not volume.

**Status: early.** `generator/` has landed, and `ingest/` has its source-table
contracts and the validator that applies them. The remaining directories exist and
each carries a README stating what that layer is and is not responsible for, but no
module has landed in them. Read the READMEs for intent; read this file for what is
actually true today.

## Shape

```
generator ──▶ raw (Parquet) ──▶ staging (Parquet, PySpark) ──▶ mart (Postgres, dbt)
                                                                      │
                                    anomaly model ──▶ LLM investigation ──▶ Streamlit
```

| Directory | Responsibility |
|---|---|
| `generator/` | Synthetic ledger data, with a switch for every failure mode the tests need. Writes CSV to `data/source/`, reproducible from a seed |
| `ingest/` | Contract validation, watermarked incremental merge, run records |
| `transform/spark/` | SCD2 loading, the point-in-time join, monthly aggregation |
| `transform/dbt/` | Relational models, tests, lineage |
| `ml/` | Anomaly detection over monthly balances |
| `insight/` | The LLM investigation loop and its golden-set evaluation |
| `app/` | Streamlit application; queries the mart, computes nothing |
| `dags/` | Airflow DAGs: daily run, backfill, evaluation |
| `tests/` | pytest suites |

## Boundaries

**Postgres** is the only external service the repository talks to today. It runs in a
container declared in `compose.yaml`, pinned to `postgres:18`. Connection parameters
come from the environment; `.env.example` records the shape and `.env` is ignored.

Every service this platform grows — Airflow, Spark — is added to the same
`compose.yaml` by the task that needs it. The host machine edits code and runs tests;
it does not run services. See `docs/adr/0004-services-run-in-containers.md`.

**DeepSeek V4 Flash** is called by the insight layer once that layer exists. See
`docs/adr/0001-llm-for-the-insight-layer.md`.

## Conventions that are not obvious from the code

**Python 3.13**, declared in `pyproject.toml` and mirrored in the CI workflow. A test
asserts the two agree, because they had already drifted apart once before anything
checked.

**Dependencies live in `pyproject.toml` only**, installed with
`pip install -e '.[dev]'` — the same command locally, in CI, and in any image. The
core set is small; `spark`, `dbt`, `ml` and `app` are declared but installed by
nobody yet. The task that first needs one of them is the task that makes it install.

**Tests that need the database fail when it is absent — they never skip.** A skipped
test reports success, and a green CI run that verified nothing defeats the point of
having gates at all. The failure message names the command that starts the
containers.

**Test data is generated, never committed.** `generator/` writes the five source
tables to `data/source/`, which is ignored — `raw/` is the layer that exists after
ingest, and the two names are not interchangeable. The same seed gives byte-identical
files, so a scenario test can plant a failure and assert on it. Each failure mode
draws from its own random stream, so switching one on leaves the data belonging to the
others where it was. Anomalies are constructed, never found by scanning: rows are
streamed and forgotten, so nothing that requires holding them is possible.

Two anomaly shapes exist and they are opposites. A concentrated one puts the increase
into a few large entries; a long-tail one spreads it across hundreds of small ones, so
the entry count stays flat and the largest twenty account for under a tenth of the
rise. The long-tail switch therefore raises amounts on a dedicated account rather than
appending rows — a steady count is the shape's diagnostic feature, not an accident of
implementation. See docs/adr/0007-long-tail-anomaly-changes-amounts.md.

**The first quality gate is contract validation, and it is the only one that exists.**
`ingest/validate.py` applies a contract to a source file: an added column warns and
the run continues, and a missing column, a reordering, a value that no longer fits its
declared type or rule, a repeated primary key, or a broken row constraint fails it.
That asymmetry is one rule for all five tables and it lives with the validator, not in
the contracts. The library returns a report rather than raising, because a warning and
a failure have to reach the caller through the same call; `python -m ingest.validate`
is what turns an incompatible report into a non-zero exit. Rows stream and findings
are capped per table, so the gate can guard a table it could not hold. Until dbt
lands there is no lineage graph, so a failure says the downstream impact is unknown
rather than omitting it. See docs/adr/0009, 0010, 0011 and 0012.

**What ingest expects is stated separately from what the generator emits.**
`generator/schema.py` is the truth for the one, `ingest/contracts/*.yaml` for the
other, and nothing under `contracts/` imports the generator. A contract derived from
its producer cannot catch the producer changing; two independent statements can
disagree, and a test that compares them is what turns a drift into a decision. The
contracts' business rules are checked against data with every failure-mode switch on,
because a late entry or a cost centre that moved department is a legitimate business
event, not malformed input. See docs/adr/0008-contracts-are-written-by-hand.md.

**Everything in this repository is written in English** — code, comments, commit
messages, identifiers, configuration, and documents.

## Where decisions live

Decision records are in `docs/adr/`, one decision per file. Search them rather
than reading the directory.
