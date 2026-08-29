# Validation streams the rows and caps what it reports

summary: Rows are read one at a time and never collected; findings are collected up to a per-table cap and counted exactly beyond it; the primary-key check is the one check that holds memory, and that cost is stated rather than hidden.

## Context

`gl_entry` is meant to reach tens of millions of rows — that is problem six, and the
generator was built streaming for it (ADR 0006). The validator reads the same files.
A gate that has to hold the table it is guarding cannot guard the table the platform
is actually built for.

Two things can grow without bound here, and they are different problems. One is the
rows. The other is the findings: a source that changed `amount_dr` to a formatted
string produces one violation per row, and ten million error messages is not a
diagnosis.

## Decision

**Rows stream.** `csv.DictReader` is consumed one row at a time and no row is kept
after it has been checked.

Asserting this needs more care than the generator's equivalent test, and the reason is
the primary-key set below: peak memory here *does* grow with row count, because the
keys do, so the generator's "ten times the rows, at most twice the peak" bound would
fail against a correct implementation. The term that separates the two is row *width*.
A validator that collected rows would grow with it; the key set does not touch it. So
the test holds the row count and the key set constant and triples the width of every
row, requiring the peak to stay within a fifth of where it started. A second test
asserts the key growth directly rather than pretending it away.

**Findings are capped per table, and counted exactly beyond the cap.** The default is
50, settable with `--max-findings`. Past the cap the validator stops collecting but
keeps scanning and keeps counting, so the report says "50 shown, 8,412,003 more"
rather than "at least 50". The full scan is the price of an honest number, and it is
one sequential pass over a file that was going to be read anyway.

**Header findings are never dropped by the cap.** They are few, they are the
diagnosis, and a cap that could hide "the `currency` column is gone" behind fifty row
violations would defeat the gate.

**The primary-key check holds every key seen.** This is the one check that cannot be
done in bounded memory, and it is kept rather than dropped: a repeated
`(entry_id, version)` is what makes the idempotent merge produce a wrong answer
instead of a duplicate, and it is cheaper to catch here than downstream. For
`gl_entry` at ten million rows this is a set of ten million string tuples, which is
hundreds of megabytes — large, bounded, and not something to discover during a scale
run.

## Reasoning

The cap and the exact count look like the same decision and are not. Capping alone
gives a readable report that understates the damage, which invites "only fifty rows
are bad, ship it". Counting alone gives an accurate number attached to an unreadable
report. Both together is the only combination that tells someone what happened and
how much of it there is.

The primary-key memory is recorded here rather than left as an implementation detail
because it is the thing that will break first at scale, and when it does the fix is a
decision — sort-based detection, a Bloom filter with a second pass, or moving the
check into the merge where the keys are already indexed — not a bug fix. Whoever hits
it should find this record and pick, rather than rediscover that the validator was
never bounded and assume it was an oversight.
