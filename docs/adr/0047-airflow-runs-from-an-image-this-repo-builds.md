# Airflow runs from an image this repository builds, against the Postgres already there

summary: Three services — api server, scheduler, dag processor — on `LocalExecutor`,
built from `apache/airflow:3.3.1-python3.13` with a JRE and this project installed, using
a second database inside the Postgres `compose.yaml` already declares.

## Context

`docs/adr/0004` says every service after Postgres is added to the same `compose.yaml` by
the task that needs it, and that the host edits code and runs tests rather than running
services. This is that task for Airflow.

Two constraints shape what can be written. Airflow's own reference `docker-compose.yaml`
is a nine-service deployment on `CeleryExecutor` — scheduler, dag processor, api server,
worker, triggerer, init, its own Postgres, Redis, and an optional Flower — which is a lot
of moving parts for a platform whose nightly work is seven commands in order. And
whatever executes the steps needs what the steps need: a JDK for Spark, which
`docs/adr/0028` keeps in-process rather than in a cluster, and dbt.

## Decision

`compose.yaml` gains three services:

- `airflow-apiserver` — the REST API and the UI
- `airflow-scheduler`
- `airflow-dag-processor`

The DAG processor is listed because Airflow 3 requires it as a standalone process, not
because this deployment wants a fourth thing to run. `AIRFLOW__CORE__EXECUTOR` is
`LocalExecutor`, under which the scheduler runs tasks in its own process — so there is no
worker service, no Redis, and no broker to keep alive.

The metadata database is a second database inside the existing `postgres` service, not a
second Postgres. It is named separately from the mart's so that dropping one never
touches the other.

**It is created by a one-shot `airflow-init` service, not by a script in
`/docker-entrypoint-initdb.d/`.** The Postgres image runs those scripts only when it
starts against an empty data directory, and leaves a pre-existing database untouched —
so on every machine that has run this project before, an init script would silently not
run and Airflow would come up pointing at a database that does not exist. `airflow-init`
creates the database if it is absent and runs `airflow db migrate`, and the three
services depend on it having completed.

A `Dockerfile` in the repository root extends `apache/airflow:3.3.1-python3.13`, adds a
JRE, and installs this project with `pip install -e ".[spark,dbt]"`. Airflow 3.3.1
documents support for Python 3.10 through 3.14 and this project requires 3.13, so the
Python-suffixed tag is pinned rather than the default one, which tracks a different
version.

`apache-airflow` still does not appear in `pyproject.toml`. `docs/adr/0004` stands
unedited.

## Reasoning

Trimming the reference compose file is the whole of the first decision, and the reference
file's own documentation calls it a local-development quickstart rather than a
deployment. `CeleryExecutor` buys distribution across worker machines; there is one
machine here, and the cost of that purchase is Redis, a worker service and a broker that
can fail independently of the thing it is brokering for. `LocalExecutor` is what this
workload is, and it removes two services and a failure mode from a file whose job is to
be startable.

Reusing the Postgres service and adding a database, rather than adding a Postgres, is the
same reasoning one level down: a second image and a second volume to hold a scheduler's
bookkeeping, next to an identical one already running, is a cost with nothing bought.
Separate databases keep the isolation that matters — the mart is dropped and rebuilt
routinely, and Airflow's history must not be inside the blast radius.

Building an image rather than mounting the project into the stock one is what makes the
JDK and dbt reproducible. `docs/adr/0028` decided Spark runs inside the process that
imports it, which makes a JRE a toolchain requirement of anything that runs a step; a
stock Airflow image has none, and installing one at container start is a step that can
fail differently every morning.

Pinning `apache/airflow:3.3.1-python3.13` rather than `:3.3.1` matters more than it
looks. The unsuffixed tag carries whatever Python was the newest supported at that
release, which is 3.12 today; this project's `requires-python` is `>=3.13`, so the
default tag would fail to install it, and would fail at image build rather than at some
later and less obvious point. The version pair is exactly the kind of thing two different
tasks edit independently, which is why a test asserts the base image's Python satisfies
`requires-python` — the same guard that already exists for CI's Python version.

The cost is a Dockerfile and an image build in a repository that had neither, and a
`docker compose up -d` that now takes noticeably longer the first time. It is stated
here rather than discovered.
