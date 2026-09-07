# transform/spark

PySpark jobs for the work that has to scale.

**Landed:** `session.py`, which builds the local SparkSession, and `scd2.py`, which
turns the two effective-dated source dimensions into validity intervals.

```
python -m transform.spark.scd2 --raw data/raw --staging data/staging
```

`--table` scopes the run to one dimension. Exit 0 on success, 2 for a usage error.

**Not built yet:** the point-in-time join that attributes each entry to the hierarchy
and exchange rate in effect on its accounting date, broadcasting the dimensions to
avoid shuffling the fact table; monthly aggregation with period-over-period and rolling
windows; small-file compaction after each write.

## The SCD2 load

The source declares its own history. `dim_account_src` and `dim_cost_center_src` key
on `(code, effective_date)` and carry every version in every extract, so `valid_from`
is the date a change took effect in the business rather than the date this pipeline
noticed it, and one run over one extract reconstructs the whole chain.

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
