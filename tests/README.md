# tests

pytest suites over the logic the engineering problems turn on. What is covered today:
the generator's failure modes and its two anomaly shapes, the source-table contracts
and the validator that applies them, the raw layer's layout and checksum, the
watermarked merge's idempotency, and the run record. The point-in-time join, SCD2
loading and backfill scoping are not covered because they are not written yet.

Scenario tests use the generator to plant a specific failure and assert the pipeline
responds to it — a long-tail anomaly that sorting by amount cannot find, a source
table that loses a column, a batch loaded three times leaving the row count and
checksum unchanged.

Tests that need Postgres fail rather than skip when it is absent. A skipped test
reports success, and a CI run that verified nothing would come back green. See
`docs/adr/0004-services-run-in-containers.md`.
