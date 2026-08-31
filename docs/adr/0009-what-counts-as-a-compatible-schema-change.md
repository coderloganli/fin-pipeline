# An added column is the only compatible schema change

summary: The declared columns must all be present and in their declared relative order; any other column is an addition, which warns and lets the run continue. A missing column, a reordering, a value that no longer fits its declared type or rule, a repeated primary key, or a broken row constraint all fail the run.

## Context

The contracts under `ingest/contracts/` state what ingest expects each source table
to look like. They deliberately do not state what to do when reality disagrees —
that is one rule for all five tables, and ADR 0008 left it to the validator. This is
that rule.

The distinction the platform needs is not "did anything change" but "did something
change that can break a reader". Upstream systems add fields constantly. A pipeline
that fails on every one of them gets switched off within a month, and a pipeline that
fails on none of them corrupts reports silently. Both failure modes are named in the
problem this ticket exists to solve.

## Decision

**Compatible — warn and continue:** a column appears in the header that no contract
column claims, while every declared column is present and their relative order is
unchanged. The run proceeds and the report carries a warning naming the table and
the added column.

**Incompatible — fail the run:**

- a declared column is absent from the header
- the declared columns appear in a different relative order than the contract states
- a header column name appears twice
- the source file for a contracted table is absent entirely
- a value does not parse as its declared type
- an empty field appears in a column declared `nullable: false`
- a value falls outside `allowed`, below `min`, or is written to the wrong `scale`
- the primary key repeats
- a `row_constraint` is broken

There is no third category. Nothing is "compatible with a warning" except the added
column, and nothing incompatible is downgradeable by configuration.

**Relative order, not position.** A column added in the middle of the header is still
an addition, because the declared columns are still in their declared order around
it. This is stricter than checking the set of names and looser than checking exact
positions, and both of those alternatives are wrong: the set alone would accept a
header that swapped `amount_dr` and `amount_cr`, and exact positions would fail a
producer that inserted a field in the middle, which is an ordinary thing for a
producer to do.

**A header failure stops the table.** When the header is already incompatible, the
rows are not read. The diagnosis is complete without them, and reading on would bury
the one finding that matters under a violation for every row of a column that is not
there.

## Reasoning

The asymmetry between an added column and a dropped one is the whole point, and it is
not arbitrary. A reader that does not know about a new column is unaffected by it —
that is what makes the change safe, and it is why the warning exists rather than
nothing at all: safe is not the same as unremarked, and someone should decide whether
the new field belongs in the contract. A reader that depends on a column which has
disappeared is already broken; the only question is whether it finds out at the gate
or three layers downstream in a number nobody double-checks.

Reordering sits with the incompatible changes because CSV readers are positional more
often than they admit, and because the contract declares order explicitly rather than
declaring a set. A contract that states order and then tolerates its violation is
stating nothing.

Putting the value-level checks in the same category as the structural ones is the part
worth defending. A changed type is invisible in a CSV header — `291.01` and `two
hundred` are both text — so a gate that only compared column names would pass a source
that had changed `amount_dr` from a number to a formatted string, which is exactly the
"loose enough to fail silently" half of the problem. The contracts already declare
enough to catch it. Declining to use those declarations would leave the platform with
a schema gate that cannot detect a schema change of type, which is one of the three
changes the requirement names.

The rules being checked here were validated against data generated with every business
switch on before they were written down, so an entry posted six weeks late or a cost
centre that moved department passes this gate. See ADR 0008. That property is what
keeps this from becoming a gate people route around.
