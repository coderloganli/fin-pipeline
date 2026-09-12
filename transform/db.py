"""How this project reaches Postgres.

One resolver, used by the loader, by the dbt project's environment and by the test
suite. Two copies of this would drift, and the one that drifted would be the one
nothing ran.

Settings resolve the way Compose resolves them: a real environment variable wins, then
`.env`, then the built-in default. Reading `.env` matters because Compose reads it and
a Python process otherwise would not - editing it would move the database the container
publishes while everything else kept connecting to the old one.

The repository root is an argument rather than a module global. `tests/` moves it to
test the `.env` precedence, and a resolver reading its own globals would quietly stop
being affected by that.

When the database is absent the failure names the command that starts it. It is not a
skip: a skipped test reports success, and a CI run that verified nothing would come back
green. See docs/adr/0004-services-run-in-containers.md.
"""

import os
from pathlib import Path

__all__ = [
    "DEFAULTS",
    "START_COMMAND",
    "DatabaseUnavailable",
    "parse_env_file",
    "settings",
    "connect",
    "connection_from",
]

REPO_ROOT = Path(__file__).resolve().parent.parent

START_COMMAND = "docker compose up -d"

# The two schema names are settings for the reason docs/adr/0034 gives: running the
# test suite must not overwrite the schemas a developer has been looking at in the same
# database. `landing` rather than `staging`, because docs/adr/0026 already owns that
# name for the Parquet layer.
DEFAULTS = {
    "POSTGRES_HOST": "127.0.0.1",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "fin_pipeline",
    "POSTGRES_USER": "fin_pipeline",
    "POSTGRES_PASSWORD": "fin_pipeline",
    "POSTGRES_LANDING_SCHEMA": "landing",
    "POSTGRES_MART_SCHEMA": "mart",
    # The anomaly flags are not in the mart and not built by dbt: a promotion drops the
    # mart schema and renames a build into place, and this layer must not be able to
    # stop the mart being published. See docs/adr/0050.
    "POSTGRES_ANOMALY_SCHEMA": "anomaly",
}


class DatabaseUnavailable(RuntimeError):
    """Raised instead of the driver's own error, so the message says what to do."""


def parse_env_file(path: Path) -> dict[str, str]:
    """Read KEY=VALUE lines, ignoring blanks and comments."""
    if not path.is_file():
        return {}
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def settings(repo_root=None, env=None) -> dict[str, str]:
    """Connection settings: environment, then `.env`, then the default.

    Both inputs are arguments. Passing them is what lets a test move the `.env` it
    reads without reaching into this module.
    """
    root = Path(repo_root) if repo_root is not None else REPO_ROOT
    environment = os.environ if env is None else env
    from_file = parse_env_file(root / ".env")
    return {
        key: environment.get(key) or from_file.get(key) or default
        for key, default in DEFAULTS.items()
    }


def connect(host: str, port: int | str, dbname: str, user: str, password: str):
    """Open a connection, or fail with a message that names the start command."""
    import psycopg

    try:
        port = int(port)
    except (TypeError, ValueError) as failure:
        raise DatabaseUnavailable(
            f"POSTGRES_PORT must be a number, got {port!r}. "
            f"Check .env against .env.example."
        ) from failure

    try:
        return psycopg.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=5,
        )
    except psycopg.OperationalError as failure:
        raise DatabaseUnavailable(
            f"cannot reach Postgres at {host}:{port} as {user}. "
            f"Start it with `{START_COMMAND}` from {REPO_ROOT}, "
            f"or point POSTGRES_* at another database. Driver said: {failure}"
        ) from failure


def connection_from(values: dict[str, str] | None = None):
    """A connection built from resolved settings, for a caller that has no opinion."""
    values = values or settings()
    return connect(
        host=values["POSTGRES_HOST"],
        port=values["POSTGRES_PORT"],
        dbname=values["POSTGRES_DB"],
        user=values["POSTGRES_USER"],
        password=values["POSTGRES_PASSWORD"],
    )
