# The mart is promoted by renaming a schema, not by building in place

summary: A run builds the mart into `<mart>__b<run_id>` and renames it into place in one
transaction when `dbt build` comes back green; a build that fails leaves its schema
behind for inspection and changes nothing a reader can see.

## Context

`docs/adr/0034` recorded the gap and left it: dbt builds the models and then runs the
tests over them, so a gate that goes red does so after the table it guards has been
written. `dbt build` stops there and exits non-zero, but the mart schema is holding the
figures that failed the gate until the next successful build replaces them.

The six gates are not the problem. They catch what they were built to catch, and
`tests/test_mart_gates.py` proves each of them can turn red. What is missing is that
catching a failure and publishing it are not, today, mutually exclusive.

`docs/product.md` says breaking is better than drifting. A run that stops and leaves
wrong figures where an analyst reads them has done neither.

## Decision

A run builds into a schema of its own and the schema is renamed into place on success.

**The build schema is `<mart>__b<run_id>`.** The run id goes in verbatim, so the schema
name is what somebody pastes into `python -m ingest.runs --run`, and the prefix is what
makes the schemas of one mart findable as a set.

**Promotion is one transaction.** Drop the mart schema if it is there, rename the build
schema to it, and do the same for `<schema>_dbt_test__audit`. Postgres makes DDL
transactional, so a reader sees the mart it had or the mart it is getting, never a
partial one, and a promotion that fails halfway leaves the old mart intact.

**Promotion runs under `lock_timeout`.** `tests/conftest.py` already records what
happens without one: a connection sitting idle in a transaction holds a lock on what it
read, which blocks DDL "indefinitely and with nothing raised". A promotion that hangs is
worse than one that fails — the run never reports at all — so the lock wait is bounded
and the timeout is raised as `PromotionBlocked`.

**`model_row_count` is copied into the build schema and swapped with it.** The table is
the drift gate's memory (`docs/adr/0036`) and it is the one thing in the mart that
cannot be recomputed from its inputs. `prepare` copies the live mart's copy into the
build schema before dbt starts; dbt's `is_incremental()` reads `adapter.get_relation` at
execute time, so a table already there is one it appends to.

**A failed build's schema is kept until a build succeeds.** `store_failures` is on
project-wide precisely so a red gate can say what it caught; dropping the schema on
failure would throw that away at the moment somebody wants it. The sweep therefore runs
after a promotion, not before a build, and drops every build schema of that mart other
than the one just promoted.

**The swap belongs to the `dbt-build` pipeline step.** `python -m pipeline daily`,
`python -m pipeline backfill` and the two DAG tasks get it. A bare `dbt build` still
writes `POSTGRES_MART_SCHEMA` directly.

## Reasoning

Renaming rather than copying, because a copy is a second full write of the mart and it
is not atomic — the reader would see tables replaced one at a time, which is the failure
being fixed rather than a smaller version of it.

Renaming rather than a view layer over versioned schemas — `mart` as a set of views
repointed on promotion — because repointing a view is itself a schema change under the
same lock, so it buys nothing, and it would put a second name for every table into a
repository whose argument is that a layer's name means one thing.

**Copying `model_row_count` forward rather than keeping it outside the swap.** A durable
state schema that promotion never touches was the alternative, and it was declined. The
row for a build is written before the drift gate runs, so a build that fails the gate has
already appended its count; outside the swap that row stays, and the baseline the gate
compares against would include builds that were rejected. Keeping the table inside the
swap makes the discard do that work: the failed build's row is in the schema that was
thrown away. The rule is then one rule — what is promoted is what happened — rather than
a rule plus a compensating delete that has to be got right on the failure path.

**Sweeping on success rather than on every build.** Sweeping at the start of the next
build was the first shape, and it needs to know which discard is newest. It cannot:
`docs/adr/0019` gives a run id a timestamp to the second and a random suffix, and
`ingest/runs.py` says in as many words that two ids from the same second "sort
arbitrarily with respect to each other". Ordering build schemas by name would therefore
sometimes keep the older failure. Sweeping on success needs no order — everything but
the promoted schema goes — and it is the better rule for the reader it exists for: under
the other one, a failure's evidence is destroyed by the very next attempt at the thing
that failed, which is usually minutes later and before anyone has looked. What it costs
is that a run of consecutive failures leaves one schema each until one succeeds.

**Keeping the failed schema rather than dropping it.** The counter-argument is that the
database accumulates. That is bounded by how long a broken pipeline is left broken, and
each schema's name says which run it belongs to.

**Not making the swap unavoidable.** Routing every `dbt build` through the promotion
would have closed the last way to write the mart directly, and it would have cost the six
gate scenarios the thing they assert on — what a red gate left behind. A scenario that
can only see a schema that was thrown away is not testing the gate.

## Consequences

**The mart schema name has a budget.** Postgres truncates an identifier at 63
characters and dbt appends `_dbt_test__audit` to the target schema; with `__b` and a
23-character run id, `POSTGRES_MART_SCHEMA` may be at most 21 characters. Longer, and
`transform/promote.build_schema` refuses rather than letting Postgres truncate — the
failure that shape produces is "relation already exists" on a test that is perfectly
correct, which `tests/conftest.py` had already been bitten by once.

**A run without a run id cannot build the mart.** The step reads `context.run_id`, which
`pipeline/run.run_step` sets for every step under both entry points. A caller that
reached the step some other way is told so rather than given a schema name no record
mentions.

**The drift gate's baseline is now a history of promoted builds.** That is a narrowing
of what `docs/adr/0036` describes, and it is the one it wanted: a build whose figures
were rejected does not set the expectation for the next one.

**One database, checked as well as resolved.** The DDL here runs on one connection and
the models are built by a subprocess that connects for itself. A caller that hands in a
connection to another database would have this drop a schema where nothing was built, so
the database is compared before anything runs.

**One database, resolved once.** `pipeline/dbt.environment()` passed the process
environment and the two schema names, and `profiles.yml` falls back to its own defaults
for the rest — so a connection named only in `.env` reached `transform/load.py`, which
resolves through `transform/db.settings()`, and not dbt. That was already wrong and this
decision makes it dangerous: prepare and promote would act on one database while dbt
built in another, and the promotion's `DROP SCHEMA ... CASCADE` would land in the wrong
one. `environment()` now layers `db.settings()` under the schema names, which is what
`tests/conftest.py` was separately doing to compensate.

**Every promotion statement runs inside one transaction, entered before anything else
runs on the connection.** psycopg opens an implicit transaction at the first statement on
a connection that is not in autocommit, and `transaction()` inside one is a SAVEPOINT: it
releases on exit and commits nothing. A single read before the block therefore turns a
promotion into a no-op that reports success - the build schema is there, dbt filled it,
and closing the connection rolls the rename back. This is a shape to preserve rather than
a detail, which is why it is written into `transform/promote.py` beside the constant it
constrains.

**The sweep is the one step whose failure does not fail the run.** By the time it runs
the mart is published and correct. A discard that could not be dropped is untidy;
failing there would stop `clear-affected`, leave the affected periods owed, and have the
next run redo work that had already been done properly. It is reported in the step
detail and the next successful build sweeps it. Everything before the sweep does fail the
run, and everything after the build schema is named attaches it to the failure, so a
blocked promotion reaches the run record as more than an error string.

**A failed build is visible in two places.** The run record's `dbt-build` step carries
the build schema and `promoted: false`; the schema itself carries the offending rows in
its audit schema. Neither of them is the mart.
