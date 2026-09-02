# fin-pipeline

A finance data platform where the hard problems are data engineering ones: point-in-time correctness, slowly changing dimensions, late-arriving corrections, idempotent replay, schema contracts, and partitioned scale. An LLM layer sits on top as a consumer of the warehouse, not as its centrepiece.

**Status: the ingest layer is built; everything downstream of it is not.** The
generator, the source-table contracts and their validator, the watermarked idempotent
load, and the run record have landed. `transform/`, `ml/`, `insight/`, `app/` and
`dags/` carry a README stating what each is for and no module yet. `docs/architecture.md`
is the file that says what is actually true today; the rest of this one describes what
the platform is for. No measured numbers are published yet.

## Why this exists

Any pipeline can read a CSV, clean it, and write it to a database. Finance data is chosen because its difficulty is in time semantics rather than volume:

- Account hierarchies, cost-centre ownership, and exchange rates all change over time, so a March transaction must be reported against the hierarchy and rate that were in effect in March.
- Adjusting entries arrive weeks after a period closes, and a correction is not the same thing as a restatement.
- Re-running a historical report must reproduce the original figures, or nobody will trust it.
- Every published figure must trace back to the source rows and the pipeline run that produced it.

## Architecture

```
generator ──▶ raw (Parquet, partitioned by period) ──▶ staging (Parquet, PySpark) ──▶ mart (Postgres, dbt)
                        │                                      │                             │
                 schema contracts                    SCD2 · as-of join                   dbt tests
                 idempotent merge                    late-arrival backfill                lineage
                                                                                             │
                                        anomaly detection (scikit-learn) ─▶ LLM explanations ─▶ Streamlit
```

Airflow orchestrates the daily run and the backfills. Each directory carries a README stating what that layer is and is not responsible for.

| Layer | Storage | Responsibility |
|---|---|---|
| `raw` | Parquet, partitioned by accounting period | Source data landed unchanged, plus ingest metadata |
| `staging` | Parquet | Type normalisation, deduplication, SCD2 dimensions |
| `mart` | Postgres | Point-in-time attributed facts and aggregates, served to the application |
| `serving` | Streamlit | Interaction and presentation only; no computation |

## The seven engineering problems

1. **Point-in-time correctness.** Effective-dated dimensions and FX rates are joined against the transaction date, so restated reports reproduce their original numbers.
2. **Type 2 dimensions.** Attribute changes close the previous version and open a new one, with no overlapping or missing validity intervals.
3. **Late arrivals and corrections.** An adjusting entry backfills only the accounting periods it affects, and restatements preserve the as-reported view alongside the as-restated one.
4. **Idempotent replay.** A watermarked incremental merge keyed on entry version makes reruns produce identical output.
5. **Schema contracts.** Additive source changes pass with a warning; incompatible changes fail the run in CI and name the downstream models they would break.
6. **Partitioned scale.** PySpark broadcasts dimensions for the point-in-time join and compacts small files to keep the nightly run inside its window.
7. **Streaming and batch agreeing.** Entries are also consumed one at a time off Kafka into an intraday view, which reuses the batch attribution rather than reimplementing it, and a reconciliation check fails the build when the two disagree per account.

## Layout

```
generator/    synthetic ledger data, with switches for every failure mode the tests need
ingest/       contract validation, watermarked incremental merge, run records
transform/    spark/ for the point-in-time join and aggregation; dbt/ for models, tests, lineage
ml/           anomaly detection over monthly balances, with time-series backtesting
insight/      LLM explanations with mandatory source citations, and the golden-set evaluation
app/          Streamlit self-service application
dags/         Airflow DAGs for the daily run, backfills, and evaluation
tests/        pytest suites
docs/         architecture.md, and the decision records under adr/
```

## Running it locally

Requires Docker and Python 3.13.

```
docker compose up -d          # Postgres, on 127.0.0.1:5432
pip install -e ".[dev]"
pytest -q
```

Connection settings come from the environment. The defaults in `.env.example`
match what `compose.yaml` starts, so nothing needs setting to run the suite; copy
it to `.env` only if you want different values. A test asserts the two agree.

Tests that need the database are marked `db` and **fail rather than skip** when it
is absent — a skipped test reports success, and a CI run that verified nothing
would come back green. The failure message names the command that starts it. To
work without the containers running:

```
pytest -m "not db" -q
```

Run `pytest`, not `python -m pytest`. The `-m` form puts the working directory on
`sys.path` and the bare form does not, so a test module that imports another one can
pass locally and fail in CI, which runs the bare form. Matching the two is what makes
a green local run mean something.

The heavy dependencies — PySpark, dbt, scikit-learn, Streamlit — are declared as
optional groups in `pyproject.toml` but are not installed by `[dev]`, and are not
yet known to install cleanly. The task that first needs one is the task that makes
it work.

## Quality gates

Three gates are planned. **One of them exists today**, and this section says which,
because a list of gates is exactly the kind of claim worth being able to check:

- **contract validation at ingest — built.** `python -m ingest.validate` applies each
  source table's contract; an added column warns and the run continues, and a missing
  column, a reordering, a value that no longer fits its declared type or rule, a
  repeated primary key, or a broken row constraint fails it. It runs inside the pytest
  suite rather than as its own CI step, and it cannot yet name the downstream models a
  failure would break — that needs a lineage graph, and dbt has not landed. See
  `docs/adr/0012`.
- **dbt tests after transformation — not built.** Uniqueness, referential integrity,
  debit/credit reconciliation, SCD2 interval consistency, row-count drift.
- **pytest — built, over what exists.** The generator's failure modes, the contracts
  and the validator, the raw layer, and the watermarked merge's idempotency. The
  point-in-time join, SCD2 loading and backfill scoping are not covered because they
  are not written.

`pytest -q` is what CI runs, and it is the whole of CI today.

The LLM layer is evaluated against a fixed golden set whose answers are known because the generator produced the anomalies deliberately. A drop in that score fails the build in the same way a broken test does.

## Licence

MIT
