"""Building the local SparkSession, and failing legibly when Spark cannot start.

Local mode: the whole engine runs inside this process, so there is nothing to stand up
and nothing to connect to. See docs/adr/0028-spark-runs-in-process.md.

Ownership is held, not inferred. `acquire` establishes whether this process already has
a session *before* it builds one, and stops only a session it created; nothing here asks
`getActiveSession()`, which answers for the calling thread and not for the process. A
borrowed session is neither stopped nor reconfigured - `getOrCreate` would otherwise apply
this module's settings to a session somebody else owns. See docs/adr/0049.

The failure path matters as much as the success one. Somebody who has just cloned the
repository and run the tests has to be told what to install, and told it accurately -
Spark's own documentation says `java` on the PATH *or* JAVA_HOME pointing at an
installation, so a message demanding JAVA_HOME would send them to fix something that is
not broken.
"""

import os
import shutil
from contextlib import contextmanager

__all__ = ["SparkUnavailable", "SUPPORTED_JAVA", "build", "acquire"]

# Spark 4.2.0 states the versions it runs on as a list rather than a floor: "Spark runs
# on Java 17/21/25". A floor would pass a host running 18 or 24, and the failure would
# then arrive from inside the JVM rather than from this module.
SUPPORTED_JAVA = ("17", "21", "25")

HOW_TO_FIX = (
    "Spark needs a JDK. It runs on Java " + "/".join(SUPPORTED_JAVA) + ", found either "
    "as `java` on your PATH or through the JAVA_HOME environment variable pointing at "
    "an installation - either one is enough."
)


class SparkUnavailable(RuntimeError):
    """Spark could not start. Raised rather than skipped, for the reason the database
    fixture raises: a skipped test reports success, and a run that verified nothing
    comes back green."""


def _build_session(name: str):
    """The call that needs a JVM. Separated so the failure path can be exercised
    without hiding a Java installation from the process."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName(name)
        .master("local[*]")
        # These dimensions are tens of rows. The default of 200 shuffle partitions
        # would write 200 empty files and spend longer scheduling than computing.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _existing():
    """The session this process already has, or None.

    `SparkSession.active()` and deliberately not `getActiveSession()`. The latter is
    thread-local - Spark documents it as "the active SparkSession for the current
    thread" - and whether a session is somebody else's is a fact about the process, not
    about the thread that happens to be asking. `active()` tries the thread first and
    then falls back to the class-level `_instantiatedSession`, raising only when there is
    neither, and that fallback is the whole reason it is the right call.

    It answers without starting anything: with no `SparkContext` alive there is no
    thread-local to read and no default to fall back to, so a process that has never
    built a session is not made to build one to find that out.

    Private, and staying private. A public process-wide probe is a perfectly good way to
    write `borrowed = ...` again, which is the thing docs/adr/0049 abolishes.

    Only two answers are produced here: a session, or None. `PySparkRuntimeError` is
    Spark's way of saying there is neither an active nor a default session, which is the
    None. Anything else - a half-torn-down JVM, a broken Py4J bridge - is left to
    propagate as itself: it is not "Spark cannot start", and reporting it as that would
    send somebody to check their JDK over a fault that has nothing to do with one.
    """
    try:
        from pyspark.errors import PySparkRuntimeError
        from pyspark.sql import SparkSession
    except ImportError as failure:
        raise SparkUnavailable(
            "pyspark is not installed. It comes from the `spark` extra: "
            f"`pip install -e '.[spark]'`. Python said: {failure}"
        ) from failure

    try:
        return SparkSession.active()
    except PySparkRuntimeError:
        return None


def _obtain(name: str):
    """The session for this process, and whether this call is the one that created it.

    The one place the process-wide question is asked, so that `build` and `acquire`
    cannot come to different conclusions about the same process.

    Only the build is wrapped. `SparkUnavailable` means one thing - Spark could not be
    started here, and this is what to install - so a fault the probe hit on the way to
    answering must not be dressed up as that. `_existing` raises `SparkUnavailable`
    itself, with its own message, for the one case that really is an installation
    problem.
    """
    existing = _existing()
    if existing is not None:
        return existing, False
    try:
        return _build_session(name), True
    except Exception as failure:
        where = (
            f"JAVA_HOME={os.environ['JAVA_HOME']}" if os.environ.get("JAVA_HOME")
            else f"java on PATH: {shutil.which('java') or 'not found'}"
        )
        raise SparkUnavailable(
            f"could not start Spark. {HOW_TO_FIX} This process saw {where}. "
            f"Spark said: {failure}"
        ) from failure


def build(name: str = "fin-pipeline"):
    """The session for this process, or a SparkUnavailable saying what to install.

    An existing session is handed back untouched. Not merely unstopped - unconfigured
    too: `getOrCreate` applies "the config options specified in this builder ... to the
    existing SparkSession", so asking for a handle used to rewrite the shuffle width and
    the timezone of a session somebody else owned. Only a session this call creates is
    configured the way docs/adr/0028 argues.

    This never stops anything. A caller that needs the session for the length of a
    command wants `acquire`.
    """
    return _obtain(name)[0]


@contextmanager
def acquire(name: str = "fin-pipeline"):
    """The session for this process, stopped on the way out only if we created it.

    Ownership is settled before the session exists and is held by the block, so there is
    no flag for a caller to compute wrongly - which is the defect this replaces, one
    level up. See docs/adr/0049.
    """
    spark, created = _obtain(name)
    try:
        yield spark
    finally:
        if created:
            try:
                spark.stop()
            except Exception:
                # Teardown, and it is the last thing to happen either way. A session that
                # will not stop must not replace the failure on its way out of the block
                # with one about the shutdown - the body's exception is the one worth
                # reporting. `pipeline/run.py` suppresses its own teardown for the same
                # reason, and the two implementations of this rule should not differ.
                pass
