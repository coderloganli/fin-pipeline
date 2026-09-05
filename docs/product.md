# Product design

**Status.** This describes the product as designed. Of it, `generator/` and
`ingest/` have landed — synthetic entries, source-table contracts, the validator,
the watermarked incremental load and the run record. The layered models, the
quality gates beyond contract validation, the intraday path, the anomaly model,
the explanation layer and the application are designed and not yet built.
`docs/architecture.md` states what is true today; this file states what is being
built toward, and the two are not the same document.

## What this product is

A platform for a company's general ledger. Journal entries arrive from a source
system, pass through layered models, quality gates and period-correct
aggregation, and land as report tables an analyst can query. The same entries
also arrive one at a time down a streaming path, which produces an intraday view
of the day still in progress. On top sits a self-service application where
balances flagged as anomalous are explained in plain language, and every number
in an explanation can be traced back to the source rows it came from and the
pipeline run that produced them.

The problem it addresses is not moving data. It is what happens after the move:
whether the numbers are still right, whether a figure can be reproduced months
later, and whether a change upstream is noticed before it reaches a report.

Finance is the domain because finance makes those questions unavoidable. The
difficulty is in time semantics, not volume: account hierarchies and cost-centre
ownership get reorganised, exchange rates differ every day, adjusting entries
arrive weeks after a period closes, and a restated figure has to be
distinguishable from a late one. A March transaction in euros must be aggregated
with March's rate and March's org structure. Using today's is simply wrong, and
nothing about the size of the dataset makes that easier.

## Who it is for

**A finance analyst at a mid-sized company.** They close a period, review it,
answer questions about it afterwards, and need to trust what they are looking at
well enough to forward it. They do not write SQL and should not have to.

**An engineer reading the repository.** The data is synthetic and the whole thing
runs locally, so the design can be judged rather than described. That audience
shapes what gets built: the parts that are hard about ledger data — as-of joins,
slowly changing dimensions, idempotent merges, contract enforcement, lineage —
get the attention, and the parts that merely make an application pleasant do not.

## What it does

**Closes and reviews a period.** An analyst picks a period and sees balances by
cost centre and account, with anomalous rows flagged. Each flagged row carries a
sentence saying how far above its recent average it is, which suppliers or
entries drive it, and whether an adjusting entry in the period already explains
it. The constituent entries are one click away.

**Answers "how much so far" before the period closes.** The intraday view reports
the month to date. It agrees with the next day's batch report, because both use
the same hierarchy, the same rates and the same aggregation logic — the only
difference is the entries that have not arrived yet.

**Reproduces an old report.** Re-running a closed period returns what it returned
at the time, and the run can say which version of the org hierarchy and which
day's rates it used.

**Fails loudly when a source changes.** A new column, or a changed type, in the
upstream ledger table breaks the run in CI and names the downstream models that
would have been affected. It does not quietly pass wrong numbers to a report.

**Explains an anomaly, with its working shown.** Given a flagged balance, the
model-flagged row is investigated: where the excess concentrates, which supplier
or entries account for it, whether a known adjustment covers it, and the source
entry ids behind each claim. When there is no single driver — the increase is
spread across hundreds of small entries — saying so is the correct answer, and a
useful one, because it tells the analyst not to go chasing the largest entry.

## What it deliberately does not do

**No real company's data, and no real integration.** Entries come from a
generator, so the whole platform is public and runnable by anyone.

**No stream-processing framework.** The streaming path is one ingestion branch
that reuses the batch aggregation logic entry by entry. Flink, Spark Streaming,
CDC and exactly-once semantics are all out.

**No model research.** Anomaly detection uses established methods. What is being
built is how a data platform feeds and evaluates a model, not the model.

**The language model never decides what is anomalous.** That is a scikit-learn
model producing a prediction interval; a row is flagged when the actual value
falls outside it. Thresholds are not hand-picked and the language model is not
consulted. It only ever works on rows already flagged.

**The language model computes nothing.** It reads results and cannot write any
table that anything depends on. It is the one read-only leaf of the whole graph:
delete it entirely and not a single reported number changes.

**The language model does not write queries.** It chooses which direction to
investigate, not how to ask. The aggregation logic is sealed inside a fixed set
of query functions it calls, and it cannot go around them or investigate without
bound.

**It does not characterise.** It describes where a figure is high and what it
came from. Calling something an error, or fraud, is a person's job.

**No permissions, tenancy or approval flows**, and no front-end design work. They
are orthogonal to what this is meant to demonstrate.

## Principles

**A number is only as good as its lineage.** Every reported figure can name the
rows it came from and the run that produced it. A figure that cannot is a defect,
not a limitation.

**Period-correct or wrong.** Aggregation uses the hierarchy, ownership and rates
that were in force for the period being reported, never today's. This applies to
every new measure without being re-argued.

**Re-running is expected, so writes are idempotent.** Late and corrected entries
are normal, not exceptional. Merging the same entry twice must not change the
result, and re-running a period must reproduce it.

**History is closed, never overwritten.** A dimension change closes the old row
and opens a new one. Overwriting in place would make old reports drift as the
organisation reorganises, which is the failure the whole project exists to avoid.

**Breaking is better than drifting.** When a source contract is violated, the run
stops. A pipeline that keeps going and reports a wrong number is worse than one
that fails.

**The application is a way to see the data, not a place data is decided.** Logic
lives in the models. Streamlit is used because it is enough.
