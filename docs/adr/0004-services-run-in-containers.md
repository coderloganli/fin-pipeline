# Services run in containers, and the tests refuse to run without them

summary: Postgres and every service after it run in Docker Compose; tests that need the database fail with the command to start it rather than skipping.

## Context

The platform needs Postgres now, and Airflow and Spark later. The work happens on
Windows, where Airflow cannot run natively — its documentation supports POSIX systems
and directs Windows users to WSL2 or Linux containers. Whatever this project does
about services therefore has to answer for Airflow as well, not only for Postgres.

A test suite that depends on a live database also has to decide what to do when the
database is absent, and the two options are not equivalent.

## Decision

Services run in containers, declared in `compose.yaml`. Postgres is pinned to
`postgres:18`, the current stable major version. Later services are added to the same
file by the tasks that need them.

The host machine does two things: edit code and run pytest. It does not run services.
Consequently `apache-airflow` never enters the host's dependency declaration — it will
live in its own image — and the constraint that Airflow cannot run natively on Windows
never becomes this project's problem.

**Tests that need the database fail when it is missing. They do not skip.** The
failure message names the command that starts it.

## Reasoning

Containers were already how services are run here, so this record mostly writes down
an existing practice. What makes it worth recording is the second consequence: it
dissolves the Windows constraint rather than working around it. WSL2 would have been
the alternative, and it would have meant moving the whole working environment for the
sake of one component that a container handles anyway.

The skip-versus-fail choice is the part that will be questioned, because skipping is
the friendlier default and the one most suites reach for. It is wrong here. A skipped
test reports success, so a CI run with no database would come back green having
verified nothing — and this project's entire claim is that its quality assertions
block the build. A gate that passes when its subject is absent is not a gate. The cost
is a red suite for anyone who forgets to start the containers, and that cost is paid
by putting the start command in the failure message, where it is read at exactly the
moment it is needed.
