# The project targets Python 3.13

summary: Python 3.13 everywhere — local, CI, and container images — because every component the platform depends on supports it and the developer's machine already runs it.

## Context

Nothing in the repository declared a Python version except the CI workflow, which
pinned 3.11 while the machine the work happens on runs 3.13.11. Neither had been
chosen; both were defaults that had drifted apart without anyone noticing.

The version cannot be picked in isolation, because four components constrain it and
three of them are not yet installed. Each was checked against its official
documentation rather than recalled:

- **PySpark 4.2** requires Python 3.10 and above, with no upper bound stated. It
  separately requires Java 17 or later with `JAVA_HOME` set.
- **Airflow 3.2 and later** is tested on Python 3.10 through 3.14.
- **dbt Core 1.10** supports Python 3.13, specifically including the Postgres
  adapter, which is the adapter this project uses.

## Decision

Python **3.13**, declared once in `pyproject.toml` as `requires-python = ">=3.13"`
and mirrored by the CI workflow.

A test asserts that the version in `pyproject.toml` and the version in the CI
workflow agree. The drift this record exists to resolve was invisible precisely
because nothing checked for it; a note in a document would have been no better than
the silence that preceded it.

## Reasoning

Every constraint above admits 3.13, so the choice came down to what removes work
rather than what adds safety margin. Matching the machine the work happens on costs
one line in CI. Choosing 3.11 or 3.12 instead would mean installing and switching
between interpreters locally for the entire life of the project, in exchange for
nothing either Airflow or dbt asks for.

The known soft spot is PySpark. "Python 3.10 and above" is an open-ended statement,
not a declaration that 3.13 is tested, so PySpark on 3.13 is unverified rather than
confirmed. It is treated as such: `pyspark` sits in an optional dependency group
that nothing installs yet, and the first task that needs Spark is where that
assumption gets tested. If it fails there, this record changes and the version moves
— which is cheaper than choosing an older interpreter now against a risk that may
not exist.
