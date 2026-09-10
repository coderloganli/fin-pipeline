"""Create Airflow's metadata database, if it is not already there.

Run once by the `airflow-init` service before `airflow db migrate`, and not by a script
in `/docker-entrypoint-initdb.d/`: the Postgres image runs those only against an empty
data directory, so on every machine that has already run this project the script would
be skipped and Airflow would start against a database that does not exist. See
docs/adr/0047.

psycopg rather than `psql`, because this project declares psycopg as a core dependency
and the image installs it, whereas a client binary in the Airflow base image is
something we would be assuming rather than something we asked for.
"""

import os
import sys

import psycopg
from psycopg import sql


def main() -> int:
    name = os.environ.get("AIRFLOW_DB", "airflow")
    mart = os.environ["POSTGRES_DB"]
    if name == mart:
        # The mart is dropped and rebuilt routinely. Airflow's history must not be
        # inside that blast radius, and a typo that put it there would be discovered
        # the first time somebody rebuilt the mart.
        print(f"AIRFLOW_DB is {name!r}, which is the mart's own database",
              file=sys.stderr)
        return 2

    with psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=mart,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        autocommit=True,
    ) as connection:
        held = connection.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (name,)
        ).fetchone()
        if held is not None:
            print(f"database {name} is already there")
            return 0
        # Not `IF NOT EXISTS`: Postgres has no such form for CREATE DATABASE, which is
        # why this asks first rather than asking forgiveness. The name is composed as an
        # identifier rather than pasted in - it comes from the environment, and a name
        # carrying a quote would otherwise break startup in a way nobody would enjoy
        # diagnosing at three in the morning.
        connection.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        print(f"created database {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
