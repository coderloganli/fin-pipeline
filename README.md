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

## The data

A mid-size company trading in four currencies — CNY is the base, plus EUR, USD and GBP —
costed across **12 cost centres** under a handful of departments, against a chart of
around **40 accounts**. Sales, purchasing, expenses and payroll all end up posted to the
general ledger of one ERP as **vouchers**.

The platform does not connect to the ERP. **Every night the ERP exports five tables as
CSV into a landing directory**, and the platform picks them up from there.

```
business systems (sales · purchasing · expenses · payroll)
   │ posted to
ERP general ledger  ←── master data (accounts, cost centres), daily FX import
   │ exported nightly
landing area   data/source/*.csv        ← everything we are given
   │
contract validation → raw → staging → mart
```

A nightly file drop rather than a database connection or CDC, because that is what
finance data usually is: the ERP is a locked-down system of record, finance IT does not
hand out database credentials, and what you get is a file that appears every morning.

**That boundary is where this project's engineering comes from.** Upstream is a system
nobody here controls — it will be upgraded, it will gain a column, it will change a
type, and it will not tell you. The first thing that happens downstream is a contract
check. Swapping the file drop for CDC or an ERP API changes nothing below it, because
what the pipeline depends on is the contract, not the transport.

### The five tables

Three facts, two dimensions. The columns, types and constraints are declared in
`ingest/contracts/*.yaml`.

| Table | What it is | Primary key |
|---|---|---|
| `gl_entry` | **Journal entry lines** — one row is one debit or one credit within a voucher | `(entry_id, version)` |
| `gl_adjustment` | **Adjusting entries** — corrections that arrive after the period closed | `(entry_id, version)` |
| `fx_rate` | **Exchange rates**, one row per currency per day, against the base currency | `(currency, rate_date)` |
| `dim_account_src` | **Chart of accounts**, effective-dated | `(account_code, effective_date)` |
| `dim_cost_center_src` | **Cost centres** and the department each belongs to, effective-dated | `(cc_code, effective_date)` |

`gl_entry` carries `entry_id`, `version`, `accounting_date`, `posted_at`, `account_code`,
`cost_center_code`, `currency`, `amount_dr`, `amount_cr` and `doc_id`. `gl_adjustment`
adds `adjusts_entry_id` — which original line it revises — and `adjustment_type`, either
a `correction` or a `restatement`.

Three modelling points carry most of the difficulty:

- **`accounting_date` and `posted_at` are different dates.** The first says which period
  the entry belongs to and decides its partition; the second says when the row actually
  landed in the ERP and decides whether an incremental run reads it. Business booked on
  29 January may not post until 1 February. Confusing the two is the easiest mistake in
  this domain to make and the hardest to see.
- **A voucher is several rows and has to balance.** Rows sharing a `doc_id` are one
  unit, and each row is either a debit or a credit — never both, never neither. Debits
  that do not equal credits are bad data, and catching that is the quality gate's job.
- **Both dimensions are keyed on their effective date.** A cost centre moving to another
  department mid-year adds a row rather than changing one. Keyed on `cc_code` alone, that
  perfectly legitimate reorganisation would be rejected as a duplicate key — and it is
  the very scenario the point-in-time join exists for.

### Synthetic, and honest about it

The data is generated by `generator/`, so the repository is public and runnable, and
every failure mode is a switch: late entries, restatements, an organisational change,
unbalanced vouchers, two shapes of anomaly, schema drift. Because the anomalies are
planted deliberately, the golden set that evaluates the insight layer has known correct
answers.

As of the end of step one the generated data is **structurally real and semantically a
placeholder**: accounts are named `account 6100`, cost centres `cost centre CC-001`, and
`account_type` is assigned round-robin rather than meaning anything. There is also no
vendor and no voucher description, which the insight layer's planned
`breakdown_by_vendor` tool needs. Making the ledger read like a real one is generator
work, and generator work gets more expensive the later it is done.

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
