# The anomaly model trains on the mart, not on the landing layer

summary: `ml/` reads `<mart>.agg_monthly_balance` rather than the landing table of the
same name, so the model only ever sees figures that passed all six gates.

## Context

`agg_monthly_balance` exists twice. `transform/load.py` copies the staging Parquet into
the landing schema, and `transform/dbt/` builds a widened copy in the mart with the
account and cost-centre names resolved as of the period's close. The columns the model
needs — `accounting_period`, `account_code`, `cost_center_code`, `balance_as_restated` —
are in both.

Reading the landing table would let the anomaly step run without waiting for dbt.
Reading the mart makes it depend on a completed, promoted build.

## Decision

**`ml/` reads the mart.** The table is `<POSTGRES_MART_SCHEMA>.agg_monthly_balance`, and
the step that reads it runs after `dbt-build` has promoted (`docs/adr/0050`).

**The modelled quantity is `balance_as_restated`.** It is the basis the report shows and
the one the windowed comparison columns in the same table already derive from, per
`docs/adr/0043`.

## Reasoning

The landing table is what was copied across. The mart, since `docs/adr/0048`, is
something narrower and more useful: the last set of figures that passed all six gates.
A build whose vouchers did not balance, or whose row counts collapsed, never becomes the
mart — its schema is discarded and the previous mart stays where it is.

Training on the landing layer means training on numbers that may have failed a gate
minutes later. `docs/product.md` puts the principle plainly: breaking is better than
drifting, and a run that fails publishes nothing. A model fed from upstream of the gates
is a way for rejected figures to reach a reader anyway — as a queue of anomalies derived
from them — which is the failure the gates exist to prevent, arriving by a side door.

The cost is an ordering dependency, and it is one this repository already accepts
everywhere: `pipeline/steps.py` is a sequence precisely because these things depend on
each other.

**`balance_as_restated` rather than `balance_as_reported`.** A correction amends the
period's figure and a restatement presents a new basis alongside the original.
`docs/adr/0043` makes the restated basis the one the windowed columns derive from,
because it is the answer a report shows, and an anomaly queue that disagreed with the
report about what this month's number is would be answering a question nobody asked.

## Consequences

**`ml/` cannot run on a checkout that has never built the mart.** The failure names the
command, the way `transform/db.py` names the one that starts Postgres.

**A backfill re-judges what it rebuilt.** `BACKFILL` carries `dbt-build`, so it carries
`judge` too, and a period whose figures moved gets its flags recomputed from the figures
that replaced them. Flags are written per period and replaced wholesale for the periods
a run judged, rather than appended to, so a rebuilt period does not accumulate two
generations of flags.

**The mart's own names are available and are not used as features.** `account_name` and
`cost_center_name` are in the row; the model keys on the codes, because a name is
effective-dated and a rename would otherwise read as a new series.
