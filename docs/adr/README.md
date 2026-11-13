# Architecture decision records

One file per decision, recording the context, the choice, and what was rejected.

Fifty-five of them is more than anybody will read, so they are sorted here by what they
are for. The first group is the project's argument — these are the decisions the design
rests on, and each one is worth being asked about. The second is the record: real
decisions, correctly made, that a reader can look up when something puzzles them. The
third is implementation notes, kept so the reasoning is not lost but **not design
claims** — several of them record working out a library's behaviour rather than choosing
between alternatives.

Six records are marked superseded at the top of the file. `0026`, `0028`, `0041` and
`0049` describe the Spark layer, which was replaced by SQL; `0047` describes the Airflow
deployment, which was removed; `0037` describes a lineage artefact nobody downloaded.
Each says which half of its argument still holds.

## The project's argument

The data is synthetic so the answers are knowable, the warehouse is correct as of the
period being reported, and nothing is published until it has passed every gate.

| | |
|---|---|
| [0005](0005-deterministic-generation.md) | One seed, several independent random streams — which is what lets one failure mode be switched on without disturbing the data belonging to the others |
| [0007](0007-long-tail-anomaly-changes-amounts.md) | The long-tail anomaly raises amounts rather than adding rows — the only shape that tells a fixed slice apart from an investigation |
| [0022](0022-a-vendor-belongs-to-the-voucher.md) | A vendor belongs to the voucher, and comes from a stream of its own — the other half of what makes the two anomaly shapes separable |
| [0008](0008-contracts-are-written-by-hand.md) | Source contracts are written by hand, not derived from the generator — a check derived from its producer cannot catch the producer changing |
| [0009](0009-what-counts-as-a-compatible-schema-change.md) | An added column is the only compatible schema change |
| [0012](0012-the-validator-reports-rather-than-raises.md) | The validator returns a report; only the entry point turns it into a failure |
| [0016](0016-a-merge-rewrites-the-affected-partitions-whole.md) | A merge rewrites the affected partitions whole, and the watermark moves last — the whole of idempotent replay |
| [0019](0019-the-run-record-is-an-append-only-event-log.md) | The run record is an append-only event log — so a run that died says which step it died in |
| [0027](0027-master-data-restatement-is-not-modelled.md) | Master-data restatement is not modelled, and what that forecloses |
| [0029](0029-a-rate-is-a-slowly-changing-dimension.md) | An exchange rate is a slowly changing dimension, and the fact build has one join shape |
| [0035](0035-a-contract-names-the-dbt-source-it-feeds.md) | A contract names the dbt source it feeds — which is what makes the impact list possible |
| [0039](0039-the-affected-period-set-is-recorded-then-interpreted.md) | The affected-period set is recorded by ingest and interpreted by transform |
| [0043](0043-as-reported-and-as-restated-are-one-row.md) | as-reported and as-restated are two columns of one row, bridged by a delta |
| [0048](0048-the-mart-is-promoted-by-renaming-a-schema.md) | The mart is promoted by renaming a schema — so a red gate publishes nothing |
| [0050](0050-anomaly-flags-are-written-outside-the-mart.md) | Anomaly flags are written outside the mart, after it is published — the model cannot hold back the report |
| [0054](0054-the-anomaly-layer-gates-behaviour-not-score.md) | The anomaly layer gates behaviour and the anomalies it was given, not its score |
| [0056](0056-the-planted-growth-sits-where-the-model-can-see-it.md) | The planted growth sits where the model can see it — ground truth outside the window being evaluated is not ground truth |

## The record

Real decisions, looked up rather than led with.

| | |
|---|---|
| [0003](0003-how-dependencies-are-declared.md) | Dependencies are declared in pyproject.toml, with the heavy ones optional |
| [0004](0004-services-run-in-containers.md) | Services run in containers, and the tests refuse to run without them |
| [0013](0013-a-rate-is-not-an-amount.md) | A rate is not an amount: six decimal places, drawn as an integer |
| [0014](0014-the-contract-declares-the-watermark-column.md) | The contract declares the watermark column, and for entries it is `posted_at` |
| [0015](0015-raw-lands-every-column-as-text.md) | Raw lands every column as text, one Parquet file per accounting period |
| [0017](0017-the-raw-checksum-is-over-rows-not-bytes.md) | The raw checksum is computed over rows, not over file bytes |
| [0018](0018-raw-rows-carry-two-run-identifiers.md) | A raw row carries two run identifiers, not one |
| [0023](0023-raw-never-forgets-a-primary-key.md) | The raw layer never forgets a primary key |
| [0024](0024-an-open-interval-ends-at-a-sentinel-date.md) | A validity interval's open end is a sentinel date, not null |
| [0025](0025-the-surrogate-key-is-derived-not-sequential.md) | The surrogate key is derived from the version it identifies, not issued in sequence |
| [0026](0026-staging-is-typed-and-rebuilt.md) | *Superseded.* Staging is typed, and rebuilt rather than accumulated |
| [0028](0028-spark-runs-in-process.md) | *Superseded.* Spark runs in the test process, not in a container |
| [0030](0030-the-rate-series-is-a-bounded-random-walk.md) | The rate series is a bounded random walk, and it is not published at the weekend |
| [0031](0031-base-currency-conversion-rounds-at-the-line.md) | Base-currency conversion rounds at the line, not at the total |
| [0032](0032-a-monthly-balance-is-signed-by-its-normal-side.md) | A monthly balance is signed by the account's normal side, as it stood in the period |
| [0033](0033-the-monthly-grid-is-dense.md) | The monthly grid is dense, and null is not zero |
| [0034](0034-the-mart-is-loaded-by-copy.md) | The mart is loaded into Postgres by COPY |
| [0036](0036-row-count-drift-is-measured-against-a-snapshot.md) | Row-count drift is measured against a snapshot the mart keeps itself |
| [0040](0040-a-dirty-period-drags-its-windows-with-it.md) | A dirty period drags its windows with it, and the closure is derived from them |
| [0041](0041-selective-recomputation-bounds-writes-not-reads.md) | *Superseded.* Selective recomputation bounds what is written, not what is read |
| [0042](0042-an-adjustment-is-a-fact-of-its-own.md) | An adjustment is a fact of its own, not a row in fct_gl_entry |
| [0044](0044-the-run-record-covers-every-step.md) | The run record covers every step, not only the load |
| [0045](0045-the-run-id-is-the-platforms-not-the-orchestrators.md) | The run identifier belongs to the platform; the orchestrator's is recorded beside it |
| [0046](0046-orchestration-logic-lives-outside-the-dag.md) | *Partly superseded.* The orchestration logic is a Python package; the DAG is a declaration |
| [0047](0047-airflow-runs-from-an-image-this-repo-builds.md) | *Superseded.* Airflow runs from an image this repository builds |
| [0049](0049-spark-session-ownership-is-held-not-inferred.md) | *Superseded.* Spark session ownership is held, not inferred |
| [0051](0051-the-anomaly-model-trains-on-the-mart.md) | The anomaly model trains on the mart, not on the landing layer |
| [0052](0052-the-prediction-interval-is-fitted-not-added.md) | The prediction interval is fitted, not added around a point prediction |
| [0053](0053-the-backtest-splits-on-the-period.md) | The backtest splits on the accounting period, not on row position |
| [0055](0055-features-come-from-the-series-own-past.md) | Every feature comes from the series' own past, and that sets the demo's period range |

## Implementation notes

Kept so the reasoning is not lost. **These are not design claims** — most record working
out how a library behaves, or a refactor, rather than a choice between alternatives.

| | |
|---|---|
| [0002](0002-python-version.md) | The project targets Python 3.13 |
| [0006](0006-generator-writes-streaming-csv.md) | The generator streams rows to CSV under data/source/ |
| [0010](0010-the-validator-owns-the-rule-primitives.md) | The rule primitives live in the validator, not in the contract loader |
| [0011](0011-validation-streams-and-caps-its-findings.md) | Validation streams the rows and caps what it reports |
| [0020](0020-the-source-file-hash-belongs-to-the-run.md) | The source file hash belongs to the run, not to the row |
| [0021](0021-the-chart-of-accounts-is-modelled-on-the-standard.md) | The chart of accounts follows the accounting standard, two levels deep |
| [0037](0037-the-lineage-artefact-is-rendered-from-the-manifest.md) | *Superseded.* The lineage artefact is rendered from the manifest, not by dbt docs |
| [0038](0038-a-mart-row-carries-the-run-that-landed-it.md) | A mart row carries the run that landed the source, not the build that shaped it |
