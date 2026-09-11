"""Who owns the Spark session, and who is allowed to stop it.

The four commands under `transform/` used to decide by inference:

    borrowed = session.active() is not None

`session.active()` was `SparkSession.getActiveSession()`, which Spark documents as
returning "the active SparkSession for the current thread". A session that exists in the
process but is not active on the calling thread therefore read as `None`, while
`session.build()` went through `getOrCreate` and handed that very session back - so the
command stopped a session belonging to somebody else. Ownership has to be established
before the session is built, and held; these are the cases that say so.

Cases 5-17 of task.md. Cases 1-4 live beside the commands they are about, in
`test_scd2.py`, `test_facts.py`, `test_balances.py` and `test_backfill.py`. See
docs/adr/0049-spark-session-ownership-is-held-not-inferred.md.
"""

import shutil

import pytest

from conftest import (TEST_PERIODS, in_a_fresh_process, run_off_thread,
                      session_is_stopped)
from transform.spark import session


# --- the probe --------------------------------------------------------------

def test_the_probe_finds_the_session_this_process_has(spark):
    """Case 5. The ordinary case, on the thread that built it."""
    assert session._existing() is spark


def test_the_probe_answers_for_the_process_not_the_calling_thread(spark):
    """Case 6. The defect reduced to one assertion.

    On a plain thread the JVM thread-local is empty while the process's session is alive.
    A probe that read the thread-local would say "no session here" and a caller would go
    on to stop one it did not create.
    """
    from pyspark.sql import SparkSession

    thread_local, process_wide = run_off_thread(
        lambda: (SparkSession.getActiveSession(), session._existing())
    )

    assert thread_local is None, "the condition this case is about did not hold"
    assert process_wide is spark


def test_the_probe_starts_no_jvm_to_say_there_is_no_session():
    """Case 7. `None` on its own would not show that nothing was started to find out.

    Answering by starting a JVM would make every command pay for a session it was about
    to be told it did not need.
    """
    printed = in_a_fresh_process("""
        from pyspark import SparkContext

        from transform.spark import session

        print(SparkContext._active_spark_context is None)
        print(session._existing() is None)
        print(SparkContext._active_spark_context is None)
    """)

    assert printed.split() == ["True", "True", "True"]


# --- acquire ----------------------------------------------------------------

def test_acquire_hands_back_the_session_the_process_already_has(spark):
    """Case 8. It borrows, and it leaves what it borrowed running."""
    with session.acquire("fin-pipeline-borrowing") as borrowed:
        assert borrowed is spark

    assert not session_is_stopped(spark)


def test_acquire_from_a_thread_without_an_active_session_still_borrows(spark):
    """Case 9. The same as case 8 under the condition the old inference got wrong."""
    def borrow_and_report():
        with session.acquire("fin-pipeline-off-thread") as borrowed:
            return borrowed is spark

    assert run_off_thread(borrow_and_report) is True
    assert not session_is_stopped(spark)


def test_acquire_stops_the_session_it_created():
    """Case 10. The guard against the fix becoming a leak instead.

    `stop()` clears `_instantiatedSession`, so its absence afterwards is the observable.
    """
    printed = in_a_fresh_process("""
        from pyspark.sql import SparkSession

        from transform.spark import session

        with session.acquire("fin-pipeline-creating") as created:
            print(SparkSession._instantiatedSession is not None)
            print(created.sparkContext._jsc.sc().isStopped())
        print(SparkSession._instantiatedSession is None)
        print(created.sparkContext._jsc is None
              or created.sparkContext._jsc.sc().isStopped())
    """)

    assert printed.split() == ["True", "False", "True", "True"]


def test_a_failure_inside_the_block_still_stops_the_session_and_is_not_swallowed():
    """Case 11. Teardown must not eat the failure it is tearing down after."""
    printed = in_a_fresh_process("""
        from pyspark.sql import SparkSession

        from transform.spark import session

        class Deliberate(RuntimeError):
            pass

        created = []
        try:
            with session.acquire("fin-pipeline-failing") as spark:
                created.append(spark)
                raise Deliberate("from inside the block")
        except Deliberate as failure:
            print("propagated:" + str(failure).replace(" ", "_"))
        print(SparkSession._instantiatedSession is None)
        print(created[0].sparkContext._jsc is None
              or created[0].sparkContext._jsc.sc().isStopped())
    """)

    assert printed.split() == ["propagated:from_inside_the_block", "True", "True"]


# --- the commands reach the primitive ---------------------------------------

COMMANDS = {
    "scd2": ("transform.spark.scd2", "fin-pipeline-scd2"),
    "facts": ("transform.spark.facts", "fin-pipeline-facts"),
    "balances": ("transform.spark.balances", "fin-pipeline-balances"),
    "backfill": ("transform.backfill", "fin-pipeline-backfill"),
}


@pytest.fixture
def workspace(clean_staging, tmp_path):
    """A raw and a staging layer of this test's own, copied from the built ones.

    The commands write into staging, and one of them writes into raw's state file, so
    they cannot be pointed at the session-scoped fixture directly.
    """
    def copy():
        raw_dir, staging = tmp_path / "raw", tmp_path / "staging"
        if not raw_dir.exists():
            shutil.copytree(clean_staging.raw, raw_dir)
            shutil.copytree(clean_staging.staging, staging)
        return raw_dir, staging
    return copy


@pytest.mark.parametrize("command", sorted(COMMANDS))
def test_a_command_in_a_process_of_its_own_stops_what_it_started(command, workspace):
    """Case 12. Case 10 proves the primitive stops what it creates; this proves the
    commands actually reach it. One that quietly kept calling `build` would leak a JVM
    while every borrowed-session case above still passed."""
    raw_dir, staging = workspace()
    module, _ = COMMANDS[command]
    argv = {
        "scd2": f'["--raw", {str(raw_dir)!r}, "--staging", {str(staging)!r}]',
        "facts": f'["--raw", {str(raw_dir)!r}, "--staging", {str(staging)!r}]',
        "balances": f'["--staging", {str(staging)!r}, "--periods", {TEST_PERIODS!r}]',
        "backfill": (f'["--raw", {str(raw_dir)!r}, "--staging", {str(staging)!r}, '
                     f'"--periods", {TEST_PERIODS!r}, "--force"]'),
    }[command]

    printed = in_a_fresh_process(f"""
        from pyspark.sql import SparkSession

        import {module} as command

        code = command.main({argv})
        print("exit", code)
        print(SparkSession._instantiatedSession is None)
    """)

    assert printed.split()[-3:] == ["exit", "0", "True"]


# --- borrowing does not reconfigure -----------------------------------------

@pytest.mark.parametrize("setting, mine, theirs", [
    ("spark.sql.shuffle.partitions", "4", "7"),
    ("spark.sql.session.timeZone", "UTC", "Asia/Shanghai"),
])
def test_borrowing_a_session_does_not_rewrite_its_configuration(
        spark, setting, mine, theirs):
    """Cases 13 and 14. `getOrCreate` is documented to apply the builder's options to the
    session it hands back, so merely asking for a handle used to rewrite settings on a
    session somebody else owned. Not stopping a borrowed session and not reconfiguring it
    are the same rule."""
    assert theirs != mine
    before = spark.conf.get(setting)
    spark.conf.set(setting, theirs)
    try:
        session.build("fin-pipeline-borrowing")

        assert spark.conf.get(setting) == theirs
    finally:
        spark.conf.set(setting, before)


def test_a_session_this_module_creates_is_configured_the_way_the_record_argues():
    """Case 15. The other half: not reconfiguring a borrowed session must not turn into
    not configuring a created one. See docs/adr/0028."""
    printed = in_a_fresh_process("""
        from transform.spark import session

        spark = session.build("fin-pipeline-creating")
        print(spark.conf.get("spark.sql.shuffle.partitions"))
        print(spark.conf.get("spark.sql.session.timeZone"))
        print(spark.sparkContext.master)
        print(spark.conf.get("spark.ui.enabled"))
        spark.stop()
    """)

    assert printed.split()[-4:] == ["4", "UTC", "local[*]", "false"]


# --- what the stage-8 review found ------------------------------------------

def test_a_session_that_will_not_stop_does_not_replace_the_failure_it_follows(spark):
    """A body that raised and a `stop()` that then raises: the body's exception is the
    one worth reporting, and teardown must not overwrite it with news about the shutdown.
    `pipeline/run.py` suppresses its own teardown failure for the same reason, and two
    implementations of one rule should not differ. Found by the stage-8 review."""
    class Deliberate(RuntimeError):
        pass

    class Unstoppable:
        def stop(self):
            raise RuntimeError("the JVM is not listening")

    def obtain_an_unstoppable_session_we_own(name):
        return Unstoppable(), True

    original, session._obtain = session._obtain, obtain_an_unstoppable_session_we_own
    try:
        with pytest.raises(Deliberate):
            with session.acquire("fin-pipeline-unstoppable"):
                raise Deliberate("the work itself went wrong")
    finally:
        session._obtain = original

    assert not session_is_stopped(spark)


def test_a_fault_in_the_probe_is_not_reported_as_a_missing_installation(monkeypatch):
    """`SparkUnavailable` means one thing: Spark could not be started here, and this is
    what to install. A broken JVM bridge met on the way to *asking* whether a session
    exists is not that, and dressing it up as that would send somebody to check a JDK
    that is fine. Found by the stage-8 review."""
    class BridgeIsBroken(RuntimeError):
        pass

    def refuses():
        raise BridgeIsBroken("py4j gateway is gone")

    monkeypatch.setattr(session, "_existing", refuses)

    with pytest.raises(BridgeIsBroken):
        session.build("fin-pipeline-broken-bridge")


def test_pyspark_missing_is_reported_as_the_installation_problem_it_is(monkeypatch):
    """The other half: the one probe failure that really is an installation problem says
    so, and names the extra to install rather than sending someone to their JDK."""
    import builtins

    real_import = builtins.__import__

    def without_pyspark(name, *args, **kwargs):
        if name.startswith("pyspark"):
            raise ImportError(f"No module named {name!r}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_pyspark)

    with pytest.raises(session.SparkUnavailable) as failure:
        session._existing()

    assert "pyspark is not installed" in str(failure.value)
    assert "[spark]" in str(failure.value)


def test_a_nested_acquire_borrows_from_the_one_that_created_the_session():
    """Ownership is settled per call, so nesting is safe: the outer call creates, the
    inner finds it and borrows, and only the outer stops. Found by the stage-8 review,
    which asked whether nesting could stop a session out from under its own caller."""
    printed = in_a_fresh_process("""
        from pyspark.sql import SparkSession

        from transform.spark import session

        with session.acquire("outer") as outer:
            with session.acquire("inner") as inner:
                print(inner is outer)
            print("after inner:", not outer.sparkContext._jsc.sc().isStopped())
        print("after outer:", SparkSession._instantiatedSession is None)
    """)

    assert printed.split() == ["True", "after", "inner:", "True",
                               "after", "outer:", "True"]


# --- nothing left to guess with ---------------------------------------------

def test_the_module_offers_nothing_to_infer_ownership_from():
    """Case 17. `active` is gone, and the probe that replaced it is private. A public
    process-wide probe would be a perfectly good way to write `borrowed = ...` again."""
    assert not hasattr(session, "active")
    assert session.__all__ == ["SparkUnavailable", "SUPPORTED_JAVA", "build", "acquire"]
