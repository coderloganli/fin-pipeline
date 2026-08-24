# transform/spark

PySpark jobs for the work that has to scale.

- SCD2 loading of the account and cost-centre dimensions
- the point-in-time join that attributes each entry to the hierarchy and exchange rate in effect on its accounting date, broadcasting the dimensions to avoid shuffling the fact table
- monthly aggregation with period-over-period and rolling windows
- small-file compaction after each write

PySpark rather than Scala: one language across the repository, and no JVM toolchain to maintain.
