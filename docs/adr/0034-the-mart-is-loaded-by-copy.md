# The mart is loaded into Postgres by COPY, not written by Spark

summary: A pyarrow reader and a psycopg `COPY` land the staging Parquet in a Postgres
`landing` schema that dbt declares as its sources; Spark never opens a database
connection.

## Context

`transform/spark/` writes its models to Parquet under `data/staging/`, and
`docs/adr/0026` settles that this is what staging is: typed, unpartitioned, rebuilt on
every run. dbt-postgres can only model tables that are already in Postgres. Nothing in
the repository has ever written to Postgres — `psycopg` is a core dependency that only
the test fixture uses.

So something has to move six staging models across, and where that code lives decides
what the mart layer is allowed to assume.

## Decision

`transform/load.py` reads each staging model's Parquet with pyarrow and writes it into
the `landing` schema of Postgres with `COPY`. One model, one table, same name.

**The schema is called `landing`, not `staging`.** `docs/adr/0026` has already settled
what staging is — typed Parquet under `data/staging/<model>/`, rebuilt every run — and
that decision stands untouched. A Postgres schema also called staging would put two
different things under one name in a repository whose whole argument is that a layer's
name has to mean one thing. `landing` says what it is: where the staging layer arrives
in the database so that dbt has something to declare as a source. It computes nothing
and holds no answer that `data/staging/` does not already hold.

Each table is dropped, recreated from the Parquet schema, and copied into inside a
single transaction. Postgres makes DDL transactional, so a load that fails leaves the
previous table exactly as it was rather than a half-filled one.

The column types come from the Arrow schema, not from a second hand-written
declaration: `string` and `large_string` to `text`, `date32` to `date`,
`decimal128(p,s)` to `numeric(p,s)`, `int32` and `int64` to `bigint`, `bool` to
`boolean`. Anything else stops the load rather than guessing.

Matched exactly rather than by family, and the difference is not pedantry: pyarrow's
`is_date` also matches `date64` and `is_decimal` also matches `decimal256`, and either
would land as something Postgres accepts while the binary copy wrote it wrongly. A
boundary that answers for types it has never seen is not fail-closed.

Both integer widths are listed because both occur: `agg_monthly_balance.rolling_periods`
is a 32-bit count out of Spark, and `bigint` is the right home for it.

`dim_vendor` is the one input read from `data/raw/` rather than `data/staging/`. It is
not effective-dated, `transform/spark/scd2.py` therefore does not build it, and all
three of its declared columns are strings — so it has no retyping to do and no staging
form to have. A staging model that copied it byte for byte would be the second copy of
an answer that `docs/adr/0026` argues against.

A raw-sourced input is projected to the columns its contract declares. Raw rows also
carry `_first_run_id` and `_last_run_id` (`docs/adr/0018`), and those are not projected:
ingestion metadata is reached through the run record rather than carried forward, which
is the shape `docs/adr/0020` settled. A staging-sourced input is loaded whole, because
`transform/spark/` has already decided what belongs in it.

Spark is not given a database connection.

## Reasoning

`pyarrow` and `psycopg` are both already core dependencies, so this route adds no
package and no toolchain. That matters more than it sounds: `docs/adr/0003` makes the
task that first needs a dependency responsible for making it install, and this task
already owes that debt for dbt.

Spark writing over JDBC was the obvious alternative and was declined twice over. It
needs a Postgres JDBC driver jar fetched at session start, which is a second download
path and a second thing to pin. And it would put two landing formats — Parquet and a
database — inside the modules `docs/adr/0026` describes as writing Parquet, so the
answer to "where does staging live" would stop being one answer.

A Parquet foreign-data wrapper would have skipped the copy entirely. It was declined
because `postgres:18`, the image `compose.yaml` and CI both pin, carries no such
extension. Adopting it means building and publishing an image, which is a larger change
to how this project runs than the transfer it saves.

Dropping and recreating rather than truncating and inserting follows the same rule
`docs/adr/0026` sets for staging: the layers that compute things are rebuilt. It also
means a column added to a staging model appears in Postgres without a migration, which
is the behaviour a derived layer should have.

## Consequences

The schema names are configuration, not constants. `POSTGRES_LANDING_SCHEMA` and
`POSTGRES_MART_SCHEMA` default to `landing` and `mart`; the loader reads the first and
the dbt project reads both through `env_var`. The test suite points them at
`landing_test` and `mart_test`, so running the suite does not overwrite the schemas a
developer has been looking at in the same database.

The mart is one full rebuild. Loading only the periods that changed is what
`backfill-only-affected-periods` exists for, and this decision does not stand in its
way: a load that writes whole tables can become a load that writes whole partitions
without the reader of those tables changing.

**There is no promotion boundary, and a failed build leaves what failed in place.** dbt
builds the models and then runs the tests over them, so a gate that goes red does so
after the table it guards has been written. `dbt build` stops there and nothing further
runs, but the mart schema is holding the figures that failed the gate until the next
successful build replaces them. Nothing in this repository reads that schema yet, and
the run that produced it exits non-zero — so what is missing is not a check, it is an
atomic swap.

Adding one means building into a schema of the run's own and renaming it into place on
success, which changes every schema name here and the shape of the test harness with it.
That belongs with `orchestrate-the-daily-run`, which is the ticket that decides what a
run is and what happens when one fails; it is recorded here rather than left for someone
to discover from a report.
