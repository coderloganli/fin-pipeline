"""How dbt is invoked here, stated once.

A subprocess rather than dbt's Python entry point, because what CI runs is the command,
and a gate that only fires through an in-process API is a gate whose behaviour in CI is
untested. That reasoning came from `tests/conftest.py`, which is where this used to
live - and it could not stay there, because `pipeline/` cannot import a helper under
`tests/`. Moving only the pipeline's own invocation would have left three statements of
how dbt is run instead of two, so every caller now comes through here.

The schemas are arguments rather than environment this module reads for itself: the test
suite points them at schemas of its own so that running the suite does not overwrite the
ones a developer has been looking at. See docs/adr/0034.

Two entry points, and the difference matters. `build` runs dbt and raises on a non-zero
exit; it needs no database of its own. `build_and_promote` is the promotion boundary
around it - a schema of the run's own, renamed into place only when the build comes back
green - and it is what the `dbt-build` step calls. See docs/adr/0048.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

__all__ = ["PROJECT_DIR", "DbtFailed", "WrongDatabase", "environment", "invoke",
           "build", "build_and_promote", "parse", "plain"]

# dbt colours its output whether or not it is writing to a terminal here. The record is
# read back as JSON and printed by `python -m ingest.runs`, where an escape sequence is
# noise at best and a mangled line at worst.
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def plain(text: str) -> list[str]:
    """dbt's output as lines, without the colour it wrote for a terminal."""
    return [ANSI.sub("", line).rstrip() for line in (text or "").strip().splitlines()]

PROJECT_DIR = Path(__file__).resolve().parent.parent / "transform" / "dbt"


class WrongDatabase(RuntimeError):
    """The connection promotion would run on is not the one dbt will build in."""


class DbtFailed(RuntimeError):
    """dbt returned a non-zero exit code. Carries what it said, because a failure that
    cannot say what it caught is half a failure - the same argument `store_failures`
    exists for."""

    def __init__(self, command: list[str], result: subprocess.CompletedProcess):
        self.result = result
        tail = plain(result.stdout)[-20:]
        super().__init__(
            f"dbt {' '.join(command)} exited {result.returncode}\n"
            + "\n".join(tail)
            + (f"\n{result.stderr.strip()}" if result.stderr else "")
        )


def environment(landing: str, mart: str) -> dict:
    """The environment dbt reads, over this process's own.

    The connection settings as well as the two schema names, and that is not tidiness.
    `profiles.yml` falls back to its own defaults for anything the environment does not
    carry, while `transform/load.py` resolves through `transform.db.settings`, which
    reads `.env` too - so a database named only there reached the loader and not dbt.
    That was already wrong; with docs/adr/0048 it would mean promoting in one database
    having built in another, and the promotion drops a schema.

    Resolved settings go underneath rather than on top: a variable already in this
    process's environment wins, which is the precedence `transform.db.settings` itself
    applies.
    """
    from transform import db

    values = dict(db.settings())
    values.update(os.environ)
    values["POSTGRES_LANDING_SCHEMA"] = landing
    values["POSTGRES_MART_SCHEMA"] = mart
    return values


def invoke(args: list[str], *, landing: str, mart: str,
           project_dir: Path = PROJECT_DIR) -> subprocess.CompletedProcess:
    """Run one dbt command and hand back what it did. Raises for nothing."""
    return subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", *args,
         "--project-dir", str(project_dir), "--profiles-dir", str(project_dir)],
        env=environment(landing, mart),
        capture_output=True,
        text=True,
    )


def build(*, landing: str, mart: str, args: list[str] | None = None) -> dict:
    """`dbt build`: the models, then the gates over them.

    Raises on a non-zero exit, because a caller that returned quietly would let
    `clear-affected` run over a mart that failed its gates. `invoke` is there for a
    caller that wants to inspect a failure instead.

    This writes `mart` directly. `build_and_promote` is what a pipeline step calls.
    """
    command = ["build", *(args or [])]
    result = invoke(command, landing=landing, mart=mart)
    if result.returncode != 0:
        raise DbtFailed(command, result)
    return {
        "exit_code": result.returncode,
        "output_tail": plain(result.stdout)[-5:],
    }


def build_and_promote(*, landing: str, mart: str, run_id: str,
                      connection=None, args: list[str] | None = None) -> dict:
    """Build into a schema of this run's own, and rename it into place if it is green.

    A build that fails changes nothing a reader can see: its schema stays where it was
    built, named in what this raises, with its offending rows still readable in the
    schema's `_dbt_test__audit`. The sweep runs after a promotion, so those discards
    survive until a build succeeds - which is when nobody needs them any more.

    The failure carries `step_detail`, which `pipeline/run.py` reads off an exception and
    records. That is how the build schema reaches the run record without a second
    mechanism.

    See docs/adr/0048.
    """
    from transform import db, promote

    schema = promote.build_schema(mart, run_id)
    values = db.settings()
    owned = connection is None
    connection = connection or db.connection_from(values)
    try:
        _same_database(connection, values)
        try:
            promote.prepare(connection, mart, schema)
            result = build(landing=landing, mart=schema, args=args)
            promote.promote(connection, mart, schema)
        except Exception as failure:
            # Every failure from here carries the schema, not just dbt's. A promotion
            # that was blocked, or a run id that named a schema nobody built, leaves
            # something behind too - and a run record holding only an error string is
            # exactly what this task exists to stop.
            _attach(failure, {"build_schema": schema, "promoted": False})
            raise

        # After the rename `schema` is the mart and is no longer a build schema, so what
        # is left to sweep is every discard. Naming it anyway says which one was kept.
        #
        # The sweep is the one step whose failure does not fail the run. The mart is
        # published and correct at this point; a discard that could not be dropped is
        # untidy, and failing here would stop `clear-affected`, leave the periods owed
        # and have the next run redo work that was already done properly. It is reported
        # instead, and the next successful build sweeps it.
        detail = {**result, "build_schema": schema, "promoted": True}
        try:
            detail["swept"] = promote.sweep(connection, mart, keep=schema)
        except Exception as failure:
            detail["swept"] = []
            detail["sweep_error"] = f"{type(failure).__name__}: {failure}"
        return detail
    finally:
        if owned:
            connection.close()


def _same_database(connection, values: dict) -> None:
    """Refuse a connection pointing somewhere dbt is not going to build.

    The DDL runs here and the models are built by a subprocess that connects for itself
    out of `environment()`. If the two are different databases, this drops a schema in
    one having built in the other - and docs/adr/0048's whole guarantee is that the mart
    a reader sees was replaced by a build that passed its gates.
    """
    expected = values["POSTGRES_DB"]
    actual = connection.info.dbname
    if actual != expected:
        raise WrongDatabase(
            f"the connection handed in is on database {actual!r} and dbt will build in "
            f"{expected!r}. Promoting across the two would drop a schema in a database "
            f"nothing was built in."
        )


def _attach(failure: BaseException, detail: dict) -> None:
    """Put the step detail on an exception, without becoming the failure.

    Reading or writing an attribute can raise as easily on an exception as on anything
    else. Losing the detail in order to report the failure is right; losing the failure
    in order to report a problem attaching the detail would be exactly backwards.
    """
    try:
        existing = getattr(failure, "step_detail", None) or {}
        failure.step_detail = {**detail, **existing}
    except Exception:
        pass


def parse(*, landing: str, mart: str) -> subprocess.CompletedProcess:
    """`dbt parse`, which writes a manifest and does not touch the database."""
    return invoke(["parse"], landing=landing, mart=mart)
