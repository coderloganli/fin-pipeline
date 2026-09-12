# The image the Airflow services run from.
#
# `docs/adr/0004` keeps `apache-airflow` out of pyproject.toml: it lives here instead,
# in an image, which is what dissolved the constraint that Airflow does not run natively
# on Windows. This is the other half of that decision - whatever executes a step needs
# what the steps need, which is a JRE for Spark (`docs/adr/0028` runs it in-process
# rather than in a cluster) and dbt.
#
# The python-suffixed tag is pinned rather than the default one. The default carries
# whatever Python was newest at that Airflow release - 3.12 today - and this project
# requires 3.13, so the unsuffixed tag would fail to install it. Airflow 3.3.1 documents
# support for Python 3.10 through 3.14. `tests/test_compose.py` asserts this tag's
# Python satisfies `requires-python`, because the two are edited by different tasks.
# See docs/adr/0047.
FROM apache/airflow:3.3.1-python3.13

# The JRE goes in as root; everything after it runs as the airflow user, which is what
# the base image expects and what owns the paths pip installs into.
USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# The project, then the install. Copying pyproject.toml on its own first would cache the
# Spark and dbt layer across source edits, and it does not work here: `[tool.setuptools]
# packages` is an explicit list - deliberately, see docs/adr/0003 - so pip needs every
# directory on it to exist, and stubbing them out would put a second copy of that list in
# this file for the packages list to silently drift from. A slower rebuild is worth more
# than a duplicated declaration.
#
# compose mounts the repository over this at run time, so a change to a DAG or a step is
# a change the dag-processor sees without a rebuild. It is copied as well so that the
# image is runnable on its own.
COPY --chown=airflow:root . /opt/fin-pipeline
WORKDIR /opt/fin-pipeline
RUN pip install --no-cache-dir -e ".[spark,dbt,ml]"
