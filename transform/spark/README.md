# transform/spark

PySpark jobs for the work that has to scale.

**Landed:** `session.py`, which builds the local SparkSession and hands it out for the
length of a command through `acquire`, stopping only what it started; `scd2.py`, which turns
the effective-dated sources into validity intervals; `facts.py`, which attributes each
entry to the structure in force on its accounting date; and `balances.py`, which
aggregates the result by month.

```
python -m transform.spark.scd2     --raw data/raw --staging data/staging
python -m transform.spark.facts    --raw data/raw --staging data/staging
python -m transform.spark.balances --staging data/staging --periods 2026-01:2026-12
```

`scd2 --table` scopes the run to one model. `balances --periods` sets the reporting
range; without it the job uses the fact table's span and says so.

What these four write to `data/staging/` is what `transform/load.py` copies into
Postgres for `transform/dbt/` to model. Nothing downstream recomputes any of it.

**Not built yet:** small-file compaction after each write, and the tuning that step
four measures.

## The fact build

Three joins, one shape, used three times:

    <natural key> = <natural key>  AND  accounting_date BETWEEN valid_from AND valid_to

Both halves, every time. With the range alone every entry matches every version valid
on its date — which does not raise, it multiplies, and the result still adds up to a
number somebody might publish. The dimensions are broadcast; they are tens of rows.

An entry that matched no account, cost centre or rate stops the build. 27% of generated
entries fall on a weekend, when no rate is published, so this is the failure the change
itself creates rather than a hypothetical one.

Amounts and rates are read as `DecimalType` and never as doubles, and the
base-currency figure is rounded at the line so the monthly aggregate agrees with the
entries added up. See docs/adr/0031.

## The monthly balances

Signed by the account's normal side, using the account type the period had — so a
reclassification does not flip the sign of a period that already closed. Every active
combination carries a row for every period in range, zero where nothing posted. Each
comparison is two columns: a delta that is always defined, and a percentage that is
null when the base is zero. See docs/adr/0032 and 0033.

## The SCD2 load

The source declares its own history. `dim_account_src`, `dim_cost_center_src` and
`fx_rate` key on a code and a date and carry every version in every extract, so
`valid_from` is the date a change took effect in the business rather than the date this
pipeline noticed it, and one run over one extract reconstructs the whole chain.

The exchange rate is loaded by this same code — a rate is a slowly changing attribute
of a currency. Friday's rate runs until the day before the next published one, so the
weekend falls inside it by construction rather than by a rule written for weekends.
See docs/adr/0029.

Per natural key the intervals abut exactly - the earlier one ends the day before the
later one begins - so they neither overlap nor leave a gap by construction, and exactly
one is current. The version in force ends at `9999-12-31` rather than at null, because
a null end makes `BETWEEN` evaluate to null and drops the current rows out of an inner
join with nothing raised. The surrogate key is a hash of the natural key and
`valid_from`, so it survives a rerun and a repartition.

A version the current extract has stopped carrying still takes part in the chain: the
raw layer keeps every key it has landed. What is not modelled is the source revising an
effective date it has already given.

See docs/adr/0023, 0024, 0025, 0026 and 0027.

## Why local mode

Spark runs inside the process that imports it, not in a container - it is a library
with a toolchain requirement rather than a service. It needs a JDK: Spark 4.2 runs on
Java 17, 21 or 25, found either as `java` on the PATH or through `JAVA_HOME`. Tests
that need it fail rather than skip. See docs/adr/0028-spark-runs-in-process.md.

PySpark rather than Scala: one language across the repository, and no JVM build to
maintain.
