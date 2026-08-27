# Dependencies are declared in pyproject.toml, with the heavy ones optional

summary: One `pyproject.toml` installed with pip; a small core plus named optional groups, so a task that needs Spark is the task that has to make Spark install.

## Context

The repository had no dependency declaration at all — CI installed `pytest` and
nothing else. Every remaining task in the first phase was blocked on this one, so
whatever it produces is what every later task builds on.

The platform will eventually pull in PySpark, Airflow, dbt, scikit-learn and
Streamlit. Those are large, they drag in toolchains of their own — PySpark needs a
JDK — and some of them are not yet known to work on the chosen interpreter. Making
all of them install correctly before anything else can begin would invert the point
of this task, which is to unblock the others quickly.

## Decision

A single `pyproject.toml` is the only place dependencies are declared, installed
with pip: `pip install -e '.[dev]'` locally, in CI, and in any container image.

The declaration is split:

- **Core** — what the platform needs to talk to its own database.
- **`dev`** — what running the test suite needs.
- **`spark`, `dbt`, `ml`, `app`** — declared but installed by nobody yet.

This task guarantees that core and `dev` install and that the suite runs. It
guarantees nothing about the other four groups. The task that first needs one of
them is the task that makes it work, and that is where its version floor and any
toolchain requirement get settled.

Lower bounds are set only where a specific version was verified to matter:
`pyspark>=4.2` is the documented release that was read, and `dbt-core>=1.10` is the
release confirmed to support Python 3.13 on the Postgres adapter. No upper bounds
are pinned.

## Reasoning

pip and `pyproject.toml` were chosen over uv and Poetry because they add no tool to
install before the project can be built — the same one-line command works on the
machine, in CI, and inside an image, with nothing to bootstrap first. uv is a
reasonable alternative and is among the installation methods Airflow's documentation
supports; it was declined here only because it buys speed at the cost of a
prerequisite, and this repository has no dependency resolution slow enough to notice
yet. Poetry was declined for a weaker reason still: it would sit at an angle to the
installation methods Airflow documents, and this project has no need it answers.

Declaring the heavy dependencies but not installing them is the part worth defending.
The alternative — leaving them out entirely until needed — loses the record of what
the platform is going to require, and each later task then invents its own answer.
Naming the groups now states the intent; leaving them uninstalled keeps this task
from being held hostage by a toolchain problem belonging to a task that has not
started.
