"""How the tests reach Postgres.

The connection is made inside a fixture a test asks for, never at import or
collection time. Tests that do not ask for it keep working while the containers
are stopped, which is what makes `pytest -m "not db"` useful.

When the database is absent the failure names the command that starts it. It is
not a skip: a skipped test reports success, and a CI run that verified nothing
would come back green. See docs/adr/0004-services-run-in-containers.md.
"""

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

START_COMMAND = "docker compose up -d"

DEFAULTS = {
    "POSTGRES_HOST": "127.0.0.1",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "fin_pipeline",
    "POSTGRES_USER": "fin_pipeline",
    "POSTGRES_PASSWORD": "fin_pipeline",
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


def settings() -> dict[str, str]:
    """Connection settings, resolved the same way Compose resolves them: a real
    environment variable wins, then `.env`, then the built-in default.

    Reading `.env` matters because Compose reads it and pytest otherwise would
    not: editing it would move the database the container publishes while the
    tests kept connecting to the old one.
    """
    from_file = parse_env_file(REPO_ROOT / ".env")
    return {
        key: os.environ.get(key) or from_file.get(key) or default
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


@pytest.fixture(scope="session")
def db():
    """A connection to Postgres. Only tests that ask for it pay for it."""
    values = settings()
    connection = connect(
        host=values["POSTGRES_HOST"],
        port=values["POSTGRES_PORT"],
        dbname=values["POSTGRES_DB"],
        user=values["POSTGRES_USER"],
        password=values["POSTGRES_PASSWORD"],
    )
    try:
        yield connection
    finally:
        connection.close()
