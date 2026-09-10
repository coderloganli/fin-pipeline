# A dirty period drags its windows with it, and the closure is derived from them

summary: A period made dirty by a late entry or a dimension version also dirties
`{M, M+1, M+2, M+12}`, computed from `balances.py`'s own window constants rather than
written down a second time beside them.

## Context

A late entry landing in March changes March's balance. It also changes April's
month-on-month delta, March through May's three-month rolling mean, and the following
March's year-on-year comparison — because `transform/spark/balances.py` computes those
columns with `lag(1)`, `rowsBetween(-2, 0)` and `lag(12)` over a window ordered by
accounting period.

Recomputing only the period the entry landed in would leave four other periods holding
figures derived from a balance that has since changed. Those are the "affected
cumulative views" the ticket names, and they are not a vague category: they are exactly
what the window definitions say they are.

## Decision

The window sizes become module constants — `MOM_LAG = 1`, `YOY_LAG = 12`,
`ROLLING_SPAN = 3` — and `balances.build` uses them where it previously used literals.

`balances.dirty_closure(periods)` expands a set of dirty periods by exactly those
constants: for each period M it adds M + 1 (month-on-month), M + 1 through
M + ROLLING_SPAN - 1 (the rolling mean's forward reach) and M + 12 (year-on-year). The
result is clipped to the reporting range.

Nothing else states the closure. A test asserts that changing a constant changes the
closure, so the two cannot drift apart.

**It is applied once, by the aggregate.** `transform/spark/affected.py` reports what is
owed and does not close it. Closing in both places widens the set twice over: a period
made dirty only because it is three months after a real change would then drag its own
three months with it, and a backfill of one period walks forward through the year.

**The facts are not closed at all.** An entry's attribution is a function of that entry
and the dimensions, so `fct_gl_entry` and `fct_gl_adjustment` rewrite exactly the periods
whose rows changed. Only the aggregate has windows, so only the aggregate has a
closure.

## Reasoning

The closure is a consequence of the aggregate's definition, not an independent fact
about the ledger. Writing `{M, M+1, M+2, M+12}` as a literal beside the windows would
create the standard failure of this shape: someone widens the rolling mean to six
months, the aggregate is correct, and the backfill quietly stops recomputing three of
the periods it changed. Nothing raises. The figures are simply stale, in the layer whose
entire purpose is to not be stale — and staleness is the failure mode
`docs/adr/0027` already identified as the one that does not announce itself.

Deriving it costs a function and buys the property that the aggregate and the backfill
cannot disagree about what the aggregate depends on.

The forward direction only, and that is worth stating. A dirty period M does not dirty
M - 1: no column of M - 1's row reads forward. `account_type`'s backfill over
`rowsBetween(0, unboundedFollowing)` is the one expression that does, and it applies
only to a period that posted nothing at all, whose type is borrowed rather than
computed. That is covered by the full narrow read in `docs/adr/0041` rather than by
widening the closure backwards, because widening it backwards would reach every earlier
period and there would be no closure left.

Twelve months is a long reach for one late entry, and it is the honest one: a
year-on-year column is a comparison with a figure that has changed.
