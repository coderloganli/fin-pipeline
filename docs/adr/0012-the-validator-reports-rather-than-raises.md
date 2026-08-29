# The validator returns a report; only the entry point turns it into a failure

summary: `validate_source` returns a structured report of findings and never raises on bad data; the CLI is what maps an incompatible report to a non-zero exit, and a warning is both returned and logged.

## Context

This gate has two outcomes that are not "fine" and they must be told apart by the
caller: an added column warns and the run continues, a dropped column stops it. A
function that raises can express only one of those.

The callers that do not exist yet matter here. The watermarked load, the run record,
and the Airflow DAG are all separate tickets, and each of them will want the same
finding for a different purpose — the run record wants to store the warnings, the DAG
wants an exit code, a future backfill may want to validate several tables and report
on all of them rather than stopping at the first.

## Decision

**`validate_source(source_dir, ...)` returns a `Report`.** It raises only for
conditions that are not about the data: an unreadable directory, a contract that does
not load. Bad data is a finding, not an exception.

A `Finding` carries the table, the severity (`warning` or `incompatible`), a
machine-readable kind, and a human sentence. `Report.incompatible` is the single
question the CLI asks.

**Every table is validated, even after one fails.** The report covers all five, so a
source system that changed three tables is diagnosed in one run rather than three.

**`python -m ingest.validate` is what fails.** Exit 0 when the report is clean or
carries only warnings, 1 when it is incompatible, 2 for a usage error. The mapping
from report to exit code lives in one place and is the only place that knows about
exit codes at all.

**Warnings are returned and logged.** The structured finding is what callers and
tests read; a line on stderr through the standard `logging` module is what a person
reads. The library never configures logging — it takes a logger and leaves handlers,
formatting and levels to whoever is running it. The CLI configures logging because it
is the application.

**The failure message states that downstream impact is unknown.** dbt has not landed,
so there is no lineage graph to query and no list of models to name. The message says
that in words rather than omitting the section, so that the gap is visible in the
output a person actually reads, and so the dbt ticket has one obvious place to fill
in. Silently printing nothing would let the missing half of the requirement pass for
finished.

## Reasoning

Raising is the shorter code and it is wrong for one specific reason: it forces the
warning case into the same channel as the failure case, and the entire product of this
gate is that those two cases are different. A validator that raised would have to
either raise on an added column, which fails the run for a compatible change, or
return silently, which loses the warning — and the warning is not decoration. It is
how a contract gets updated before the added column becomes a column something depends
on.

Validating every table rather than stopping at the first failure costs one full pass
over data that is already on disk and turns a sequence of runs into one. Upstream
schema changes arrive in batches, because they come from one release of one source
system.
