"""PySpark jobs.

What has landed: `session.py`, which builds the local SparkSession every job and test
uses, and `scd2.py`, which turns the effective-dated source dimensions into validity
intervals.

Spark runs in local mode, inside the process that imports it - it is a library with a
toolchain requirement rather than a service to stand up, which is the one exception to
docs/adr/0004-services-run-in-containers.md and is argued in
docs/adr/0028-spark-runs-in-process.md.
"""
