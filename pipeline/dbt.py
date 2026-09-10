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
"""

import os
import re
import subprocess
import sys
from pathlib import Path

__all__ = ["PROJECT_DIR", "DbtFailed", "environment", "invoke", "build", "parse",
           "plain"]

# dbt colours its output whether or not it is writing to a terminal here. The record is
# read back as JSON and printed by `python -m ingest.runs`, where an escape sequence is
# noise at best and a mangled line at worst.
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def plain(text: str) -> list[str]:
    """dbt's output as lines, without the colour it wrote for a terminal."""
    return [ANSI.sub("", line).rstrip() for line in (text or "").strip().splitlines()]

PROJECT_DIR = Path(__file__).resolve().parent.parent / "transform" / "dbt"


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
    """The environment dbt reads its two schema names out of, over this process's own."""
    values = dict(os.environ)
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

    Raises on a non-zero exit, because this is what a pipeline step calls and a step
    that returned quietly would let `clear-affected` run over a mart that failed its
    gates. `invoke` is there for a caller that wants to inspect a failure instead.
    """
    command = ["build", *(args or [])]
    result = invoke(command, landing=landing, mart=mart)
    if result.returncode != 0:
        raise DbtFailed(command, result)
    return {
        "exit_code": result.returncode,
        "output_tail": plain(result.stdout)[-5:],
    }


def parse(*, landing: str, mart: str) -> subprocess.CompletedProcess:
    """`dbt parse`, which writes a manifest and does not touch the database."""
    return invoke(["parse"], landing=landing, mart=mart)
