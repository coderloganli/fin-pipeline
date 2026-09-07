# A validity interval's open end is a sentinel date, not null

## Context

An SCD2 dimension row is valid over a closed interval. The row that is currently in
force has no known end, and there are two ways to write that down: leave `valid_to`
null, or set it to a date far enough in the future that no accounting date reaches it.

The interval exists to be joined on. The point-in-time join reads

    fact.accounting_date BETWEEN dim.valid_from AND dim.valid_to

which is the whole reason the dimension is modelled this way at all.

## Decision

`valid_to` on the current version is `9999-12-31`. It is never null. `is_current` is
the boolean that says a version is the one in force; the date is not asked to carry
that meaning as well.

## Reasoning

With a null end, `BETWEEN` does not evaluate to false - it evaluates to null, and the
row drops out of an inner join. Nothing is raised and nothing is logged. The rows that
disappear are precisely the current versions, which are the majority of the dimension
and the ones every recent period depends on, so the symptom is a report that is
quietly missing most of its data rather than one that fails.

Every consumer would have to write `OR valid_to IS NULL` at every use, and the failure
mode of forgetting it is invisible. Choosing between a value that is wrong in the year
9999 and a null that is wrong the first time somebody writes a straightforward join is
not a close call.

It also makes the two properties this dimension is asserted on expressible without a
null branch. "The intervals do not overlap" and "the intervals leave no gap" are one
comparison per adjacent pair - the earlier row's `valid_to` plus one day equals the
later row's `valid_from` - and that comparison is the same for every pair including
the last.

`is_current` is redundant with `valid_to = 9999-12-31` and is kept anyway, because the
question "which version is in force now" is asked constantly and a comparison against
a magic constant is a worse way to ask it than a boolean. The two are written by the
same code path from the same window, so they cannot disagree.

The cost is a magic value, and a claim about the future that is not true. It is a
claim made in a column whose meaning is "in force up to and including", where the
alternative claim - "there is no such date" - is the one that breaks arithmetic. The
value is defined once and referred to by name.

Dates rather than timestamps: the source declares `effective_date` at day resolution,
the entries carry `accounting_date` at day resolution, and inventing sub-day precision
the source does not have would be modelling a distinction that cannot arise. A change
takes effect at the start of its effective date, and the previous version ends the day
before.
