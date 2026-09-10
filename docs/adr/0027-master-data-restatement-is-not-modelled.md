# Master-data restatement is not modelled, and what that forecloses

## Context

The source dimensions are effective-dated: a change to a cost centre's department or
an account's place in the chart arrives as a new row carrying the date the change took
effect. `docs/adr/0023` makes the raw layer keep every version it is handed, and the
SCD2 loader turns those versions into validity intervals.

There is a second thing an upstream system can do, and this platform does not model
it: correct a date it has already given. The organisation is restructured in January
but nobody updates master data until an audit finds it in September, and the correction
is booked with its real effective date. If the export had already carried the change as
effective in July, the September extract restates it as effective in January.

That is the master-data twin of a distinction this platform already treats as central.
`gl_adjustment` separates a correction from a restatement, and `backfill-only-affected-periods`
is the ticket that gives reports an as-reported and an as-restated view of entries.

## Decision

Master-data restatement is out of scope. The platform models organisational change; it
does not model the source correcting when an organisational change took effect.

An extract that contradicts a version already held - the same natural key and
`effective_date`, different attributes - fails the run rather than overwriting.
See `docs/adr/0023`.

Three things follow from this and are not built:

**No system-time axis on the dimension.** A version records when it took effect in the
business, not which run first told us about it. Without restatement there is no
question that needs the second date: today's answer for March and April's answer for
March are the same answer.

**No record of a version ceasing to be asserted.** The raw layer keeps a version the
current extract no longer carries, but does not mark it as no longer carried.
Distinguishing "the source deleted this" from "the source never deleted anything"
needs the same system-time axis.

**No table format with built-in time travel.** Delta Lake and Iceberg both offer
snapshot reads, which is one way to reconstruct what a layer held at a past run.

## Reasoning

Restatement is a real thing that happens to finance master data, and modelling it
would be the strongest single demonstration this dimension could carry: a report whose
figures change while not one journal entry has changed. That is the reason it was
considered rather than dismissed.

It is out because it is not one of the parts of the real scenario this platform is
built to handle, and the platform's argument does not rest on it. Organisational
change that takes effect going forward already exercises the point-in-time join and
the SCD2 intervals, which are the two things the dimension exists for.

The boundary should be stated plainly rather than left to be discovered. This platform
can reproduce a closed period's figures against the hierarchy in force for that
period. It cannot reproduce what a report said before the source corrected itself
about that hierarchy, because it does not keep the second date that question needs.

Not building a system-time axis is worth separating from not being able to imagine it.
The pair - the date a change took effect, and the date we learned of it - is the
standard answer, and it is the answer to reach for if restatement is ever brought into
scope. It is not built because with restatement out of scope the two dates are never
independent: every version is learned when it is exported, and no version's effective
date is ever revised. A second date that is always a function of the first is a column
that can only drift.

Iceberg or Delta would supply snapshot reads without either date being modelled, and
that is the reason not to reach for one. This platform's subject is time semantics -
which hierarchy was in force, which rate applied, which period a late entry belongs
to. Time travel obtained from a table format answers a different question, about the
storage layer's own history, and adopting one would put a substantial dependency
between the reader and the thing being demonstrated. Plain Parquet keeps the semantics
in the model, where they can be read.

**One gap this exposed, and it is now closed.** Affected periods were derived from
entries' accounting dates alone. A dimension change produces no entry, so under that
mechanism it triggered no recomputation at all: a change in the hierarchy would leave
every downstream figure at its old value until something else forced a rebuild.

`backfill-only-affected-periods` closed it. A dimension change can only ever be the
insert of a new `(natural key, effective_date)` version, because all three
effective-dated tables declare `rows_are_immutable` - which is this record's own
decision, and it turns out to make the trigger exact rather than approximate. Ingest
records the inserted versions, and transform turns each into the periods its validity
interval covers, intersected with the periods that carry entries on that key. `fx_rate`
is included on the same footing as the two org dimensions: a new rate changes every
converted amount on its dates, and leaving it out would have reproduced this gap in the
one table where it is easiest to overlook. See `docs/adr/0039`.

None of that brings restatement into scope. The trigger is a version taking effect, not
a version's effective date being revised, and an extract that contradicts a held version
still fails the run.
