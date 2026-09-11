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

**Status: the batch path is complete.** `generator/` has landed. `ingest/` has its
source-table contracts, the validator that applies them, the watermarked incremental
load that lands entries in the raw layer, and the run record every load writes.
`transform/spark/` has the SCD2 loader — which builds the two dimensions and the
exchange rate alike — the point-in-time fact build, and the monthly aggregation; it is
also what first installs PySpark. `transform/load.py` copies the staging layer into
Postgres and `transform/dbt/` models it as a star with six quality gates over it; late
entries, adjustments and dimension changes recompute only the periods they affect;
`transform/lineage.py` renders the graph and answers what a column change would break.
`pipeline/` puts those in order and writes down what each one did, and `dags/` declares the
two Airflow DAGs that call it. `ml/`, `insight/` and `app/` exist and each carries a README
stating what that layer is and is not responsible for, but no module has landed in them.
Read the READMEs for intent; read this file for what is actually true today.

## Shape

```
generator ──▶ raw (Parquet) ──▶ staging (Parquet, PySpark) ──▶ mart (Postgres, dbt)
                                                                      │
                                    anomaly model ──▶ LLM investigation ──▶ Streamlit
```

| Directory | Responsibility |
|---|---|
| `generator/` | Synthetic ledger data, with a switch for every failure mode the tests need. Writes CSV to `data/source/`, reproducible from a seed. The chart of accounts follows the accounting standard two levels deep, and expense-side vouchers carry a vendor |
| `ingest/` | Contract validation, watermarked incremental merge, run records |
| `transform/spark/` | SCD2 loading, the point-in-time join, monthly aggregation |
| `transform/dbt/` | The mart: a star in Postgres, and the six gates over it |
| `transform/load.py` | Copies the staging Parquet into Postgres so dbt has sources |
| `transform/lineage.py` | Reads dbt's manifest: the impact list, and the HTML graph |
| `ml/` | Anomaly detection over monthly balances |
| `insight/` | The LLM investigation loop and its golden-set evaluation |
| `app/` | Streamlit application; queries the mart, computes nothing |
| `pipeline/` | The steps a run is made of, in order, and the record of what each one did |
| `dags/` | Airflow DAGs: the daily run and the backfill. Declarations only — see `docs/adr/0046` |
| `tests/` | pytest suites |

## Boundaries

**Postgres** is the only external service the repository talks to today. It runs in a
container declared in `compose.yaml`, pinned to `postgres:18`. Connection parameters
come from the environment, resolved by `transform/db.py` — environment, then `.env`,
then a default — and `tests/conftest.py` re-exports that resolver rather than restating
it. `.env.example` records the shape and `.env` is ignored.

**The mart lives in two schemas, and the first is not called staging.**
`POSTGRES_LANDING_SCHEMA` holds what `transform/load.py` copies across and
`POSTGRES_MART_SCHEMA` holds what dbt builds. The landing schema is `landing` rather
than `staging` because `docs/adr/0026` already owns that name for the Parquet layer, and
one name for two things is what this repository argues against everywhere else. Both are
settings so the test suite can point at schemas of its own. See `docs/adr/0034`.

**dbt runs against that Postgres and needs no toolchain of its own.** `transform/dbt/`
carries its own `profiles.yml` rather than expecting one in `~/.dbt`, so the same
command works on a machine, in CI and in an image with nothing to place first.

Every service this platform grows is added to the same `compose.yaml` by the task that
needs it. The host machine edits code and runs tests; it does not run services. See
`docs/adr/0004-services-run-in-containers.md`.

**Airflow is three of those services** — `airflow-apiserver`, `airflow-scheduler` and
`airflow-dag-processor`, the last required by Airflow 3 as a standalone process. They run
`LocalExecutor`, so there is no worker and no broker, and their metadata is a second
database inside the same Postgres. They are built from a `Dockerfile` extending
`apache/airflow:3.3.1-python3.13` with a JRE and this project installed; `apache-airflow`
is deliberately absent from `pyproject.toml`. See `docs/adr/0047`.

**Spark is the exception, and it is not a service.** It runs in local mode inside the
process that imports it, so it is a library with a toolchain requirement rather than
something to stand up. It needs a JDK: Spark 4.2 runs on Java 17, 21 or 25, found
either as `java` on the PATH or through `JAVA_HOME` — either one, and the list is a
list rather than a floor. CI installs Temurin 21. Tests that need it fail rather than skip, for the reason the database
fixture does. See `docs/adr/0028-spark-runs-in-process.md`.

**One session per process, and whoever started it is the only one who stops it.**
`session.acquire` holds it for the length of a command and stops only what that call
created; `pipeline/run.py` declares ownership with `owns_spark` and holds it across a run's
steps. Everything else — every `build` function, every step — is handed a session and
neither stops nor reconfigures it. Ownership is established before the session is built,
never inferred afterwards from `getActiveSession()`, which answers for the calling thread
and not for the process. See `docs/adr/0049`.

**DeepSeek V4 Flash** is called by the insight layer once that layer exists. See
`docs/adr/0001-llm-for-the-insight-layer.md`.

## Conventions that are not obvious from the code

**Python 3.13**, declared in `pyproject.toml` and mirrored in the CI workflow. A test
asserts the two agree, because they had already drifted apart once before anything
checked.

**Dependencies live in `pyproject.toml` only**, installed with
`pip install -e '.[dev,spark,dbt]'` — the same command locally, in CI, and in any image.
The core set is small — `psycopg`, `pyyaml`, and `pyarrow`, which ingest writes the raw
layer with. `spark` has been claimed by `transform/spark/` and `dbt` by `transform/dbt/`, and both
are in the install line rather than optional because the tests that need them fail
rather than skip, so the suite does not pass without them. `ml` and `app` are declared
but installed by nobody yet; the task that first needs one of them is the task that
makes it install.

**Tests that need the database fail when it is absent — they never skip.** A skipped
test reports success, and a green CI run that verified nothing defeats the point of
having gates at all. The failure message names the command that starts the
containers.

**Test data is generated, never committed.** `generator/` writes the six source
tables to `data/source/`, which is ignored — `raw/` is the layer that exists after
ingest, and the two names are not interchangeable. The same seed gives byte-identical
files, so a scenario test can plant a failure and assert on it. Each failure mode
draws from its own random stream, so switching one on leaves the data belonging to the
others where it was. Anomalies are constructed, never found by scanning: rows are
streamed and forgotten, so nothing that requires holding them is possible.

**A vendor belongs to the voucher, and only where a vendor makes sense.** A voucher
that debits an expense detail account and credits a payables one carries the same
`vendor_code` and `description` on both of its lines; one that debits receivables and
credits revenue leaves the vendor empty, because revenue is earned from customers rather
than paid to suppliers. The supplier dimension is built from a stream of its own, so
adding it moved no existing entry's amounts, dates or identifiers; the assignment itself
draws nothing and chooses by position within the account's category. The vendor distribution is a design
input: office supplies carries around thirty vendors so a long tail has somewhere to
spread, marketing carries two so a growth anomaly concentrates, and without that
asymmetry the two anomaly shapes would be indistinguishable under the insight layer's
`breakdown_by_vendor`. See docs/adr/0021 and 0022.

Two anomaly shapes exist and they are opposites. A concentrated one puts the increase
into a few large entries; a long-tail one spreads it across hundreds of small ones, so
the entry count stays flat and the largest twenty account for under a tenth of the
rise. The long-tail switch therefore raises amounts on a dedicated account rather than
appending rows — a steady count is the shape's diagnostic feature, not an accident of
implementation. See docs/adr/0007-long-tail-anomaly-changes-amounts.md.

**Contract validation is the first of three quality gates, and the second has
landed.**
`ingest/validate.py` applies a contract to a source file: an added column warns and
the run continues, and a missing column, a reordering, a value that no longer fits its
declared type or rule, a repeated primary key, or a broken row constraint fails it.
That asymmetry is one rule for all six tables and it lives with the validator, not in
the contracts. The library returns a report rather than raising, because a warning and
a failure have to reach the caller through the same call; `python -m ingest.validate`
is what turns an incompatible report into a non-zero exit. Rows stream and findings
are capped per table, so the gate can guard a table it could not hold. A failure now
names the models it would break, read off dbt's manifest — see the lineage entry below.
See docs/adr/0009, 0010, 0011 and 0012.

**The second gate is six dbt tests over the mart, and each one has a scenario that
turns it red.** Primary key uniqueness, referential integrity, debit and credit
balancing per voucher, SCD2 intervals that neither overlap nor gap, row-count drift, and
agreement between the base-currency and original amounts. The acceptance standard is
what a gate can stop, not that it exists: `tests/test_mart_gates.py` builds a failure for
every one of them and asserts the named test node fails. `store_failures` is on
project-wide, so a failing gate leaves its offending rows in
`<mart schema>_dbt_test__audit` — a gate that can say only that something failed, and
not what, is half a gate.

Drift is the one gate that needs a memory. `mart.model_row_count` gains one row per
counted model per build, and the gate compares the current count against the median of
the previous five, excluding the build being tested, failing outside ten percent. It
does not count itself, which is what keeps the graph acyclic. See docs/adr/0036.

**A failed build changes nothing a reader can see.** dbt builds the models and then runs
the tests over them, so a gate that goes red does so after the table it guards has been
written — which is why a run does not build the mart in place. It builds into
`<mart>__b<run_id>` and renames that schema into place in one transaction when
`dbt build` comes back green. A build that fails leaves its schema behind, named in the
run record, with its offending rows still readable in the schema's `_dbt_test__audit`;
the mart keeps the last figures that passed. The next build that succeeds sweeps them,
so evidence survives until the problem is fixed rather than until the next attempt. `mart.model_row_count` is copied into the build schema and promoted with it, so
the drift gate's baseline is a history of builds that were accepted. The promotion is
`transform/promote.py`, called by the `dbt-build` step; a bare `dbt build` still writes
`POSTGRES_MART_SCHEMA` directly, and that schema may be at most 21 characters because
Postgres truncates at 63 and dbt appends sixteen of them. See docs/adr/0048.

**The mart is a function of its inputs, so nothing in it names the build that wrote
it.** Rebuilding from an unchanged staging snapshot is identical in every column, over
every table but `mart.model_row_count`, which docs/adr/0036 excludes by name. A full
pipeline rerun is identical in every reported column and moves `source_last_run_id`,
because docs/adr/0018 has that set by whichever run wrote the partition — so the mart
checksum excludes the two provenance columns, exactly as docs/adr/0017 excludes
ingestion metadata from the raw one. Which build wrote a table is reached through
`mart.model_row_count`, not through a column on a row. See docs/adr/0038.

**The aggregate's dimension names are resolved as of the period's close, and the fact's
by surrogate key.** The staging fact carries `account_key`, `cost_center_key` and
`fx_key`, so the fact's widening is an equality join. The monthly aggregate carries
neither, and joining an SCD2 dimension on the natural key alone would match every
version valid in any period — which does not raise, it multiplies. Its as-of date is the
period's last day, the same date `balances.py` already reports `account_type` as of. The
vendor joins on `vendor_code` in the fact and does not appear in the aggregate at all,
whose grain has no vendor.

**The lineage graph is rendered from dbt's manifest, and a contract says what it
feeds.** dbt's graph starts at the landing tables; the hop from `gl_entry.csv` to one of
them happens inside `transform/spark/` and appears in no manifest, so every contract
declares a `feeds` list of dbt source names and `transform/lineage.py` walks `child_map`
downward from them. Every contract now names something: `gl_adjustment` reaches
`mart.fct_gl_adjustment`, and the empty `feeds` it used to declare is gone. The HTML artefact is
rendered here rather than by `dbt docs generate --static`, which does not embed the
artefacts it documents (dbt-labs/dbt-core#11986, open) and which dbt Docs v2 replaced
with a multi-file site. See docs/adr/0035 and 0037.

**The raw layer holds text: the columns the contract declares, and two run
identifiers.** Entries land under `data/raw/<table>/accounting_period=YYYY-MM/part-0000.parquet`, one file
per period, every column written as a Parquet string and an empty field written as an
empty string. Retyping is `staging`'s line in the table above, and a raw layer that
already reinterpreted cannot answer the question it exists for — whether the source
really said that.

**The raw layer never forgets a primary key, and absence is not deletion.** Both
load paths agree on this. A key that stops appearing in an extract is kept, because
once an extract has landed the warehouse is the only party that can still say what the
source said in March — the upstream system is authoritative about when a change took
effect, not about how long it will keep telling us. A contract may additionally declare
`rows_are_immutable`, which the two source dimensions do: a key that reappears carrying
different values is the source restating its own past, and it fails the run rather than
overwriting. See docs/adr/0023.

**The dimensions are effective-dated at the source, so their history is read rather
than inferred.** `dim_account_src` and `dim_cost_center_src` key on
`(code, effective_date)` and carry every version in every extract, so one run over one
extract reconstructs the whole history. `transform/spark/` turns those versions into
validity intervals: `valid_from` is the effective date, `valid_to` is the next
version's effective date less a day, and the version in force ends at `9999-12-31`
rather than at null — a null end makes `BETWEEN` evaluate to null and drops the current
rows out of an inner join with nothing raised. The surrogate key is a hash of the
natural key and `valid_from`, not a sequence, so it survives a rerun and a
repartition. See docs/adr/0024 and 0025.

**An exchange rate is a dimension, so the fact build has one join shape.** `fx_rate`
is loaded by the same SCD2 code as the two source dimensions: a currency's rate runs
from the day it was published until the day before the next one. Attributing an entry
is then `accounting_date BETWEEN valid_from AND valid_to`, three times — account, cost
centre, rate — rather than a range join for two of them and an equality for the third.
The rate feed does not publish at the weekend and 27% of entries are dated on one, so
this is load-bearing: an equality join would leave a quarter of the ledger with no
base-currency amount. See docs/adr/0029 and 0030.

**Conversion rounds at the line, and never through a float.** A line's base-currency
amount is rounded to two places in `fct_gl_entry`, and the monthly aggregate sums those
already-rounded figures — so an analyst who totals the detail gets the number the report
shows. Amounts and rates are cast from the raw layer's text to `DecimalType`; reading
them back as doubles would undo the whole of docs/adr/0013. See docs/adr/0031.

**A monthly balance is signed by the account's normal side, as it stood in the
period.** Expenses and assets are debit-normal, revenue, liabilities and equity
credit-normal, so every account's ordinary activity reads as a positive number that
grows and no consumer restates the rule. The account type comes from the point-in-time
attribution, so a reclassification does not flip the sign of periods that already
closed. The grid is dense — every active account and cost centre carries a row for every
period, zero where nothing posted — because a period-over-period comparison over a
sparse table silently becomes a comparison with the last month that had activity. Zero
and null are different facts and stay different. See docs/adr/0032 and 0033.

**Staging is typed; the dimensions are rebuilt and the facts are partitioned.** Raw
holds text and accumulates because it is the record; staging holds dates and booleans
and is derived from raw, so it can always be recomputed. The three dimensions are tens
of rows and are overwritten whole. `fct_gl_entry`, `fct_gl_adjustment` and
`agg_monthly_balance` are partitioned by `accounting_period`, matching the raw layout,
and a run rewrites only the partitions it has reason to — a run with no dirty set
rewrites all of them, which is the overwrite this used to be. See docs/adr/0026.

**What is not modelled: the source correcting when a change took effect.** A version
restating an effective date already given would need a second time axis — when it took
effect, and when we learned of it — and reports would have to distinguish what was
published from what is now believed. It is out of scope, an extract that contradicts a
held version fails. The gap it once exposed — a dimension change producing no entry and
therefore triggering no recomputation — is closed; see the affected-period entry above.
See docs/adr/0027.

**A run records which periods it dirtied.** Both merge paths already knew — one groups
its batch by period, the other counts an update only when a declared column actually
differs — and both used to throw it away. `ingest/affected.py` keeps it in
`data/raw/_state/affected_periods.json`, unioned across runs and cleared by the
orchestrator rather than by whatever read it, so an interrupted transform leaves the work
still owed. Two triggers, not one: an entry landing in a period, and a dimension version
taking effect over one. All three effective-dated tables declare `rows_are_immutable`, so
a dimension change can only be the insert of a new `(natural key, effective_date)`
version, which makes that trigger exact. Ingest records those inserts as observations;
`transform/spark/affected.py` turns each into the periods its validity interval covers,
intersected with the periods carrying entries on that key. `fx_rate` counts, like the two
org dimensions. See docs/adr/0039.

**A dirty period drags its windows with it, and recomputation bounds writes rather
than reads.** `balances.py` computes month-on-month with `lag(1)`, year-on-year with
`lag(12)` and the rolling mean over three periods, so a dirty period M also dirties M+1,
M+2 and M+12 — a closure computed from those constants rather than written down beside
them, because widening a window would otherwise leave the backfill quietly skipping
periods it had changed. Only the closure is written. Reading is wider: the dense grid's
membership and the type carried into an empty period are properties of the whole fact
table, so every partition is still read for two or five string columns, which is what
columnar storage costs and the same trade docs/adr/0016 made for the key sweep. The
criterion is about modification times, and a read does not change one. See
docs/adr/0040 and 0041.

**An adjustment is a fact of its own.** `gl_adjustment` was ingested and consumed by
nothing; it is now `fct_gl_adjustment`, attributed by the same three point-in-time joins
as an entry. It is not folded into `fct_gl_entry` because gate 3 requires the rows
sharing a `doc_id` to balance, and an adjustment is a single-sided delta against a
voucher that already balanced — folding it in would turn a gate red on correct data. It
carries the gates that apply to it and not that one. See docs/adr/0042.

**A report gives two bases, and one column bridges them.** `agg_monthly_balance` carries
`balance_as_reported` — entries and corrections — `restatement_delta`, and
`balance_as_restated`, their sum. A correction amends the period's figure; a restatement
keeps the original basis and presents a new one alongside it, which is the distinction
docs/product.md puts at the centre of what this platform is for. One row, one grain, so
the dense grid and the drift gate keep their current arguments. The delta is the seam: if
master-data restatement is ever brought into scope it contributes to a delta beside this
one rather than needing a mechanism of its own. The windowed columns derive from the
restated basis, because that is the answer a report shows. `balance` is gone. See
docs/adr/0043.

**The load is watermarked, and the merge is what makes a rerun free.** Each contract
names the column its table advances on: `gl_entry` and `gl_adjustment` advance on
`posted_at`, and a table that names none is loaded in full instead. A run reads the
source rows at or above the stored watermark less an overlap window, applies them to
the accounting periods they touch by primary key, and writes the watermark only after
every partition is written — so an interrupted run re-reads its window next time and
converges rather than leaving a hole. Periods the batch does not touch are never
opened. There is no `updated_at` anywhere in the source; the plan said there was, and
`posted_at` is the column that already means it. See docs/adr/0014, 0015, 0016.

**A rerun is checked by row count and checksum, and the checksum is over rows.** It
renders the declared columns of every row as text, sorts them, and hashes that — not
the Parquet bytes, which carry the writer's version, and not any ingestion metadata,
which differs between runs. See docs/adr/0017.

**Every run leaves a record, step by step, and it is written before the run is over.**
`data/raw/_state/runs.jsonl` is appended to and never rewritten: a `started` event, a
`step_started`/`step_finished` pair around each step, and a `finished` event. A step's
finished event carries what that step has to say — for `load`, each table's row counts,
watermark range and source file digest. A run that dies leaves its `started` line and the
`step_started` of the step it died in, and is reported as `interrupted` naming that step;
a record written only on the paths that worked would be missing from exactly the run
someone is investigating. `python -m ingest.runs --run` prints a run as its steps. See
docs/adr/0019 and 0044.

**The run identifier is the platform's; the orchestrator's is recorded beside it.** It
keeps the shape `docs/adr/0019` fixed, because `docs/adr/0018` stamps it on rows and the
lineage chain follows it backwards. Airflow's own run id is a field on the `started`
event, so the UI and the record name each other without the data layer gaining a second
identity. See docs/adr/0045.

**A run is a sequence of named steps, and the sequence lives outside the DAG.**
`pipeline/steps.py` declares them — `validate`, `load`, `recompute`, `mart-load`,
`dbt-build`, `clear-affected`; a backfill is the last four over an explicit period range.
`pipeline/run.py` is `open_run`, `run_step`, `close_run` and `run_pipeline`, the three
primitives separate so that one Airflow task can be one step while the record still spans
the DAG run. `clear-affected` is last for the reason the watermark moves last in
`docs/adr/0016`: what is cleared first is what goes missing when the run dies. Files under
`dags/` declare a schedule and a task graph and import nothing below `pipeline`; the suite
tests the runner directly and the DAG files by parsing them, never by importing Airflow.
Nothing on the host proves the DAGs parse under Airflow — `docker compose run --rm
airflow-dag-processor airflow dags list` does, and it is a command rather than a test. See
docs/adr/0046.

**A raw row says which run first landed it and which run last wrote it.**
`_first_run_id` survives every rewrite — a merge that reopened the partition for some
other row, an eviction, the whole-table replacement an unwatermarked table gets —
and `_last_run_id` is set by whichever run wrote the file. One column cannot answer
both "where did this row come from" and "which run wrote this file", and the second
question is the one asked while something is broken. Ingestion time and the source
digest are not on the row; they are reached from either identifier through the run
record. See docs/adr/0018 and 0020.

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
