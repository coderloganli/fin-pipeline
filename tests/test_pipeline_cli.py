"""`python -m pipeline`, and the steps' own failure paths.

The end-to-end cases in `tests/test_daily.py` drive the runner in-process, so that they
can share the session-scoped Spark fixture rather than starting a JVM per run. That
leaves the command itself untested, and the command is what a person types and what a
DAG's documentation names - so it is covered here, once through end to end and
otherwise at the level of argument handling and exit codes.

Cases 13-20 and 29-33 of orchestrate-the-daily-run are the semantics; this is the
surface over them.
"""

import subprocess
import sys

import pytest

from conftest import REPO_ROOT
from ingest import affected, runs
from pipeline import __main__ as cli
from pipeline import dbt, steps as step_list


def test_a_backfill_without_a_range_is_a_usage_error(capsys):
    """It exists to rebuild a range the affected-period set does not name. Without one
    there is nothing to say it should do, and defaulting to everything would be a full
    rebuild nobody asked for."""
    assert cli.main(["backfill"]) == 2
    assert "--periods" in capsys.readouterr().err


def test_an_unknown_pipeline_is_a_usage_error():
    assert cli.main(["nightly"]) == 2


def test_the_command_takes_a_run_id(tmp_path):
    """A caller that already chose the identifier passes it in, rather than having a
    second one generated underneath it."""
    given = runs.new_run_id()
    raw = tmp_path / "raw"

    code = cli.main(["daily", "--raw", str(raw), "--source", str(tmp_path / "nothing"),
                     "--staging", str(tmp_path / "staging"), "--run-id", given])

    assert code == 1  # there is no source directory; the point is which run recorded it
    assert runs.RunLog(raw).read()[0].run_id == given


def test_a_failing_run_exits_one_and_says_where_to_read_it(tmp_path, capsys):
    """The record already names the step and carries the message. This is the line the
    person who typed the command reads."""
    code = cli.main(["daily", "--raw", str(tmp_path / "raw"),
                     "--source", str(tmp_path / "nothing"),
                     "--staging", str(tmp_path / "staging")])

    assert code == 1
    printed = capsys.readouterr().err
    assert "python -m ingest.runs" in printed


def test_the_module_is_runnable(tmp_path):
    """`python -m pipeline` resolves and parses its arguments. Run as a subprocess
    because that is how it is run, and an entry point that only works when imported is
    an entry point whose behaviour where it is used is untested."""
    result = subprocess.run(
        [sys.executable, "-m", "pipeline", "--help"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    for name in ("daily", "backfill", "--run-id", "--orchestrator-run-id"):
        assert name in result.stdout


# --- the steps' failure paths ----------------------------------------------

def test_validate_stops_the_run_when_a_source_breaks_its_contract(tmp_path):
    """docs/product.md: breaking is better than drifting. A pipeline that kept going and
    reported a wrong number is worse than one that fails."""
    from ingest import contracts

    source = tmp_path / "source"
    source.mkdir()
    contract = contracts.load("gl_entry")
    columns = [column["name"] for column in contract["columns"]]
    # A column removed: incompatible, by docs/adr/0009.
    (source / "gl_entry.csv").write_text(
        ",".join(columns[:-1]) + "\n", encoding="utf-8")

    from pipeline import run as runner

    context = runner.Context(source_dir=source, raw_dir=tmp_path / "raw",
                             staging_dir=tmp_path / "staging")

    with pytest.raises(step_list.ValidationFailed):
        step_list.VALIDATE.run(context)


def test_a_failed_dbt_build_raises_and_carries_what_dbt_said():
    """A step that returned quietly would let `clear-affected` run over a mart that
    failed its gates - which is the one thing the ordering exists to prevent."""
    # A target that is not in profiles.yml. `--select` with no match would not do:
    # dbt exits 0 having selected nothing, and a test built on that would assert that
    # a successful build raises.
    with pytest.raises(dbt.DbtFailed) as raised:
        dbt.build(landing="landing_nope", mart="mart_nope",
                  args=["--target", "no_such_target"])

    assert "dbt build" in str(raised.value)
    assert raised.value.result.returncode != 0


def test_an_injected_load_that_fails_keeps_what_it_managed_to_do(tmp_path):
    """Inside somebody else's run the runner writes the record, but only the load knows
    which table it got to. That reaches the record through the exception rather than
    being lost."""
    from ingest import load

    source = tmp_path / "source"
    source.mkdir()
    log = runs.RunLog(tmp_path / "raw")
    run_id = runs.new_run_id()
    log.start(run_id, command="daily", source=str(source), raw=str(tmp_path / "raw"),
              tables=[], steps=["load"])

    with pytest.raises(FileNotFoundError) as raised:
        load.load_source(source, tmp_path / "raw", tables=["gl_entry"],
                         run_id=run_id, run_log=log)

    assert raised.value.step_detail["failed_table"] == "gl_entry"


def test_a_failure_that_takes_no_attributes_still_propagates(tmp_path, monkeypatch):
    """The detail is a courtesy; the failure is the thing.

    `__slots__ = ()` does not do this: an Exception subclass keeps its `__dict__`
    whatever it declares, so a test built on that would pass with the guard removed and
    prove nothing. An exception that refuses the assignment outright is what exercises
    the branch - and it raises something other than AttributeError, because a type with
    a `__setattr__` of its own can raise anything at all.
    """
    from ingest import load

    class Sealed(Exception):
        def __setattr__(self, name, value):
            raise RuntimeError(f"this exception takes no {name}")

    source = tmp_path / "source"
    source.mkdir()
    monkeypatch.setattr(load, "load_table",
                        lambda *a, **k: (_ for _ in ()).throw(Sealed("no")))

    log = runs.RunLog(tmp_path / "raw")
    run_id = runs.new_run_id()
    log.start(run_id, command="daily", source=str(source), raw=str(tmp_path / "raw"),
              tables=[], steps=["load"])

    with pytest.raises(Sealed):
        load.load_source(source, tmp_path / "raw", tables=["gl_entry"],
                         run_id=run_id, run_log=log)


def test_what_dbt_says_reaches_the_record_without_escape_codes():
    """dbt colours its output whether or not it is writing to a terminal. The record is
    read back by `python -m ingest.runs`, where an escape sequence is noise at best and
    a mangled line at worst."""
    coloured = "\x1b[0m02:59:39  \x1b[32mCompleted successfully\x1b[0m\n\x1b[0m  Done."

    assert dbt.plain(coloured) == ["02:59:39  Completed successfully", "  Done."]
