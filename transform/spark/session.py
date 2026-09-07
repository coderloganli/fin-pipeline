"""Building the local SparkSession, and failing legibly when Spark cannot start.

Local mode: the whole engine runs inside this process, so there is nothing to stand up
and nothing to connect to. See docs/adr/0028-spark-runs-in-process.md.

The failure path matters as much as the success one. Somebody who has just cloned the
repository and run the tests has to be told what to install, and told it accurately -
Spark's own documentation says `java` on the PATH *or* JAVA_HOME pointing at an
installation, so a message demanding JAVA_HOME would send them to fix something that is
not broken.
"""

import os
import shutil

__all__ = ["SparkUnavailable", "SUPPORTED_JAVA", "build", "active"]

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


def build(name: str = "fin-pipeline"):
    """A local SparkSession, or a SparkUnavailable saying what to install."""
    try:
        return _build_session(name)
    except Exception as failure:
        where = (
            f"JAVA_HOME={os.environ['JAVA_HOME']}" if os.environ.get("JAVA_HOME")
            else f"java on PATH: {shutil.which('java') or 'not found'}"
        )
        raise SparkUnavailable(
            f"could not start Spark. {HOW_TO_FIX} This process saw {where}. "
            f"Spark said: {failure}"
        ) from failure


def active():
    """The session already running in this process, or None.

    `build` goes through `getOrCreate`, so it hands back an existing session rather
    than a second one. A caller that stops what it was given would therefore tear down
    a session somebody else owns - which is what `python -m transform.spark.scd2` would
    do to the test suite's shared session if it did not ask first.
    """
    from pyspark.sql import SparkSession

    return SparkSession.getActiveSession()
