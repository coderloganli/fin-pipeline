# Spark runs in the test process, not in a container

## Context

`docs/adr/0004` says services run in containers: the host machine edits code and runs
tests, and every service the platform grows is added to the same `compose.yaml` by the
task that needs it. Postgres follows that rule.

`transform/spark/` is the first module to need Spark. Spark can be run either way - a
cluster of containers with a driver and workers, or in local mode, where the whole
thing is a library that starts a JVM inside the process that imported it.

## Decision

Spark runs in local mode, in the process that uses it. It is not added to
`compose.yaml`.

`pyspark` is installed from the `spark` extra already declared in `pyproject.toml`.
Spark 4.2.0's documentation states that it "runs on Java 17/21/25", and that running
locally needs "`java` installed on your system `PATH`, or the `JAVA_HOME` environment
variable pointing to a Java installation" - either, not both. CI installs Temurin 21,
which is on that list. The session factory raises a named error stating the requirement
in those terms when it cannot start.

PySpark's own installation page words the same requirement more loosely, as "Java 17 or
later with `JAVA_HOME` properly set". The two pages disagree in precision and the
narrower one is taken: the supported set is a list of three versions rather than a
floor, and `JAVA_HOME` is one of two ways to be found rather than a requirement.
Encoding the looser wording would have made the failure message reject an environment
that works.

Tests that need Spark fail when it is unavailable. They do not skip.

## Reasoning

ADR 0004 is about services: long-lived processes the platform talks to over a socket,
which have to be running before anything works and whose absence is an environment
problem rather than a code problem. Spark in local mode is not one. It has no
lifecycle outside the process, nothing connects to it, and it does not exist between
runs. Treating it as a service would mean standing up a cluster to compute a table of
sixty rows, and would put a network boundary in the middle of a unit test.

The rule ADR 0004 is really protecting - that the host machine is not asked to install
and run infrastructure by hand - is kept. What the host needs is a JDK, which is a
toolchain dependency of the same kind as the Python interpreter, not a service.

Naming three versions rather than a floor is deliberate. "17 or later" would pass a
host running Java 18 or 24, which Spark does not list, and the failure that follows
would arrive from inside the JVM rather than from the message written for the person
who has just cloned the repository.

Not skipping follows the same reasoning as the database fixture in `tests/conftest.py`,
and it is the reason that fixture was written the way it was: a skipped test reports
success, and a CI run that verified nothing comes back green. The failure has to say
what to install, because the person who sees it is someone who has just cloned the
repository.

Local mode is the honest choice for what is here. Nothing in this repository is large
enough to need a cluster, and a cluster in `compose.yaml` would be a claim about scale
the row counts do not support. The point at which that changes is when real numbers
are measured against a large dataset; running the same job against a real cluster is a
configuration change at the session factory, which is the one place that knows.
