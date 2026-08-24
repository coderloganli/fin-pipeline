# tests

pytest suites covering the logic that the six engineering problems turn on: point-in-time joins, SCD2 loading, backfill scoping, merge idempotency, and contract validation.

Scenario tests use the generator to plant a specific failure and assert the pipeline responds correctly, for example that an entry backdated by two months rewrites only the partitions it affects.
