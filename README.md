# fin-pipeline

A finance data platform where the hard problems are data engineering ones: point-in-time correctness, slowly changing dimensions, late-arriving corrections, idempotent replay, schema contracts, and partitioned scale. An LLM layer sits on top as a consumer of the warehouse, not as its centrepiece.

**Status: design complete, implementation in progress.** No measured numbers are published yet.

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
| `staging` | Parquet | Type normalisation, deduplication, SCD2 dimensions, contract validation |
| `mart` | Postgres | Point-in-time attributed facts and aggregates, served to the application |
| `serving` | Streamlit | Interaction and presentation only; no computation |

## The six engineering problems

1. **Point-in-time correctness.** Effective-dated dimensions and FX rates are joined against the transaction date, so restated reports reproduce their original numbers.
2. **Type 2 dimensions.** Attribute changes close the previous version and open a new one, with no overlapping or missing validity intervals.
3. **Late arrivals and corrections.** An adjusting entry backfills only the accounting periods it affects, and restatements preserve the as-reported view alongside the as-restated one.
4. **Idempotent replay.** A watermarked incremental merge keyed on entry version makes reruns produce identical output.
5. **Schema contracts.** Additive source changes pass with a warning; incompatible changes fail the run in CI and name the downstream models they would break.
6. **Partitioned scale.** PySpark broadcasts dimensions for the point-in-time join and compacts small files to keep the nightly run inside its window.

## Layout

```
generator/    synthetic ledger data, with switches for every failure mode the tests need
ingest/       contract validation, watermarked incremental merge
transform/    spark/ for the point-in-time join and aggregation; dbt/ for models, tests, lineage
ml/           anomaly detection over monthly balances, with time-series backtesting
insight/      LLM explanations with mandatory source citations, and the golden-set evaluation
app/          Streamlit self-service application
dags/         Airflow DAGs for the daily run, backfills, and evaluation
tests/        pytest suites
docs/         architecture decision records
```

## Quality gates

Three gates run in CI, and any one of them fails the build:

- contract validation at ingest
- dbt tests after transformation: uniqueness, referential integrity, debit/credit reconciliation, SCD2 interval consistency, and row-count drift
- pytest for point-in-time joins, SCD2 loading, backfill scoping, and merge idempotency

The LLM layer is evaluated against a fixed golden set whose answers are known because the generator produced the anomalies deliberately. A drop in that score fails the build in the same way a broken test does.

## Licence

MIT
