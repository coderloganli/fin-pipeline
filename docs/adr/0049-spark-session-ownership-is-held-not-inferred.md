# Spark session ownership is held, not inferred

## Context

`transform/spark/session.py` builds the local SparkSession, and four commands can each be
the thing that starts one: `python -m transform.spark.scd2`, `.facts`, `.balances` and
`python -m transform.backfill`. Each has to decide, when it is done, whether to stop the
session it was working with. Stopping one it did not start would take the engine away from
whoever did — in practice the test suite, whose session-scoped fixture every later test
depends on.

All four decided by inference:

```python
borrowed = session.active() is not None
spark = session.build(...)
...
finally:
    if not borrowed:
        spark.stop()
```

The intent was right and the test of it was not. `session.active()` was
`SparkSession.getActiveSession()`, which Spark documents as returning "the active
SparkSession for the current thread". A session that exists in the process but is not
active on the calling thread therefore reads as `None`, while `session.build()` goes
through `getOrCreate` and hands that very session back. `borrowed` is `False`, and the
command stops a session belonging to somebody else.

Nothing caught it. `test_the_command_does_not_stop_a_session_it_was_handed` asserts exactly
the right thing, but pytest calls `main` on the thread that owns the fixture's session, so
the thread-local lookup succeeds and the guess is accidentally correct. The cases that
break it are the ones nobody writes a test for by accident: a command driven from a worker
thread, or from anything that did not itself put the session on the calling thread.
PySpark ships `pyspark.InheritableThread` precisely because a plain `threading.Thread` does
not carry JVM thread-locals across.

`pipeline/run.py` had already met this and gone the other way: its `Context` carries
`owns_spark`, set by `python -m pipeline` because that is a fresh process it controls, and
`session_for` stops the session only when that flag says it may. It named the four
`__main__` blocks as an outstanding defect rather than copying them.

There is a second half to the same fault. `getOrCreate` is documented so: "In case an
existing SparkSession is returned, the config options specified in this builder will be
applied to the existing SparkSession." So `build` did not only risk stopping a borrowed
session — it silently rewrote its `spark.sql.shuffle.partitions` and
`spark.sql.session.timeZone` every time anything asked for a handle.

## Decision

Ownership is established before the session is built, and it is held by a context manager
rather than returned as a flag.

`transform/spark/session.py` gains `acquire(name)`. It asks whether this process already
has a session, builds one only if it does not, and on the way out stops the session only
when that call is the one that created it. It is the only thing in this module that calls
`stop()`, and after this change none of the four commands calls it at all. Two callers
elsewhere still stop a session, and both are correct to: `pipeline/run.py` stops the one
whose ownership it declared, and the test suite's fixture stops the one it built — the
fixture through `acquire`, so that it too stops only what it created. The four commands
use it and compute nothing:

```python
with session.acquire("fin-pipeline-scd2") as spark:
    ...
```

The question "does this process already have a session" is asked with the public
`SparkSession.active()`, not `getActiveSession()`. `active()` returns the thread's session
and, failing that, falls back to the class-level `_instantiatedSession` — a `ClassVar`,
and therefore process-wide — raising `PySparkRuntimeError` with errorClass
`NO_ACTIVE_OR_DEFAULT_SESSION` only when there is neither. That fallback is the whole
reason it is the right call.

`build(name)` returns an existing session untouched and configures only one it creates. A
borrowed session keeps the settings its owner gave it.

`session.active()` is removed, and the probe that replaces it is private. The public
surface is `SparkUnavailable`, `SUPPORTED_JAVA`, `build` and `acquire`.

## Reasoning

**A thread-local cannot answer a process-level question.** Ownership is a fact about the
process — who created the one engine running in it — and `getActiveSession()` reports a
fact about the calling thread. The two agree often enough to pass the tests anybody
thinks to write, which is what made this survive review twice.

**A caller cannot drop what it never holds.** `acquire` returning
`(spark, started_here)` would have been the smaller change and would have left every
caller a chance to drop the flag, invert it, or lose it down a branch — which is the shape
of the bug being fixed, one level up. Making the `with` block the thing that calls
`stop()` removes the decision from the caller instead of correcting it. That is a claim
about the caller, not about the mechanism: `acquire` itself still decides ownership by
looking before it builds, and the limit of that is stated at the end of this record.

**The probe is private because a public one is an invitation.** Exporting a
process-wide `existing()` would leave the next person a perfectly good way to reintroduce
`borrowed = ...`. Nothing outside this module needs to ask; the two callers that need a
session ask for one, and the one that needs to stop it says so with `with`.

**Teardown may not overwrite the failure it follows.** A `stop()` that raises inside
`acquire`'s `finally` would replace the exception on its way out of the block with one
about the shutdown, losing the thing worth reporting. It is suppressed, which is what
`pipeline/run.py` already does with its own teardown and for the same reason — two
implementations of one rule in one repository should not disagree about this.

**`SparkUnavailable` means one thing.** It means Spark could not be started here, and
here is what to install. A fault met while merely *asking* whether a session exists — a
half-torn-down JVM, a broken Py4J bridge — is not that, and is left to propagate as
itself rather than sending somebody to check a JDK that is fine. The one probe failure
that is genuinely an installation problem, `pyspark` absent altogether, raises
`SparkUnavailable` with a message naming the extra rather than the JDK.

**Not configuring a borrowed session is the same rule, not a separate tidy-up.** A caller
that must not stop what it did not start must not reconfigure it either. Leaving that half
undone would mean a command run beside the test suite still changed the suite's shuffle
width out from under it — quietly, and only visible as a performance change or a timezone
that no longer matched.

**`pipeline/run.py` is not folded into this.** It already holds ownership explicitly, and
it holds it across a list of steps rather than around one command, with `owns_spark`
declared by the entry point that knows. Making it use `acquire` would put a second
lifetime inside the one it already manages. The rule is shared; the mechanism does not have
to be. See `docs/adr/0046`.

**What is still an assumption.** The probe reads the process's state and the build
follows it, so two threads racing to create the first session could both believe they
created it. Spark in local mode is one engine per process and every entry point here is a
command with a single line of control; a lock would be machinery bought against a
situation the design does not contain. The honest statement is that this is correct for
one creator, which is what there is.
