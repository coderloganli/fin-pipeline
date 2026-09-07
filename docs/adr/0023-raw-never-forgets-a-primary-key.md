# The raw layer never forgets a primary key

## Context

The raw layer has two load paths, and until now they disagreed about what an absent
row means.

A table that declares a watermark - `gl_entry` and `gl_adjustment` - is loaded by
reading a window of the source and merging it into the periods it touches. The batch
is a window rather than the whole table, so a key that is not in the batch is simply a
key the window did not reach. That path has never deleted anything for being absent.

A table that declares no watermark - both source dimensions, `fx_rate` and
`dim_vendor` - was loaded by `write_table`, which replaced the table's contents. Its
own docstring stated the consequence: "a key the incoming rows do not carry stays
gone."

That was a defensible choice when nothing consumed the dimensions. It stops being one
the moment a dimension carries history. The two source dimensions are effective-dated:
their primary keys include `effective_date`, and a cost centre that moves department
mid-year appears in the extract as two rows. Under whole-table replacement, the day
the upstream system stops exporting the older of those two rows - because it was
replaced, because master data was purged, because the ERP itself was replaced - the
raw layer deletes it on the next run, without a warning, and every historical report
that depended on it silently changes.

`docs/product.md` states that history is closed and never overwritten, and that
re-running a closed period returns what it returned at the time. Whole-table
replacement made the first of those a statement about a dimension row's *contents*
only, and left its *existence* unprotected.

## Decision

The raw layer never removes a primary key it has landed. Absence from an extract is
not a deletion.

`write_table` becomes `merge_table`: the existing file is read, the incoming rows are
applied by primary key with the incoming row winning, and the result is sorted and
written back. A key the incoming rows do not carry is kept. For a partitioned table
the same rule applies per partition, and a partition the batch does not touch is
neither read nor removed.

`_first_run_id` continues to come from `prior_first_run_ids`, and `_last_run_id` is the
run that wrote the file, exactly as before.

A contract may declare `rows_are_immutable: true`. Under it, an incoming row whose
primary key is already held and whose other columns differ fails the run, naming the
table, the key, the columns that differ and both values. The two source dimensions
declare it.

`fx_rate` and `dim_vendor` accumulate but do not declare immutability.
`gl_entry` and `gl_adjustment` are unchanged: the incoming row still wins at the same
`(entry_id, version)`.

A source extract that is missing entirely still raises, as it did before. A source
extract that exists and holds no rows now leaves what has accumulated in place.

## Reasoning

The argument that decides it is not about upstream reliability, it is about where the
truth lives. The upstream system is authoritative about *when a change took effect* -
that is what `effective_date` is, and this platform must never re-derive it. But it is
not authoritative about *what we were ever told*, because it is under no obligation to
keep telling us. Once an extract has landed, the warehouse is the only party that can
still answer what the source said in March, and a layer that deletes on absence
forfeits that.

Making the unwatermarked path agree with the watermarked one removes a rule rather
than adding one. There is now a single sentence describing the raw layer's behaviour
on absence, and it is the same sentence for all six tables.

`rows_are_immutable` is separate from accumulation because they answer different
questions. Accumulation says a row that stops being mentioned is kept. Immutability
says a row that is mentioned again with different contents is a contradiction rather
than an update. For an effective-dated dimension the second is true by construction: a
change is supposed to produce a new `effective_date`, so the same
`(code, effective_date)` carrying different attributes means the source is restating
its own past. Letting that overwrite silently would reintroduce, one row at a time,
exactly the drift this record exists to prevent. Failing follows
`docs/product.md`'s "breaking is better than drifting", and it is a rare enough event
to be worth a person's attention.

`fx_rate` has the same shape and the same argument - `(currency, rate_date)`, and a
given day's rate does not change - but nothing consumes it yet. Declaring immutability
there is one line in its contract, and it belongs to the ticket that first joins on
it rather than to this one, which would otherwise be deciding on behalf of a consumer
that does not exist. `dim_vendor` is keyed on `vendor_code` alone with no date, so a
supplier's name genuinely does change in place and immutability would be wrong.
`gl_entry` already carries `version` for the purpose immutability would serve; whether
its merge should refuse a changed row at an unchanged version is a real question and a
different ticket's.

What this gives up is the ability to represent a deletion at all. If the upstream
system ever needs to say that a cost centre was struck from the record rather than
merely no longer exported, it has to say so in a column, and absence will still not
mean it. That is the right trade: a deletion that is declared can be modelled, and one
that is inferred from silence cannot be distinguished from an extract that failed
halfway.

The cost is that the raw layer only grows. For the four accumulating tables that is
bounded by how much history the source has, which for a chart of accounts and twelve
cost centres is not a quantity that needs managing.

The second cost is memory, and it is worth stating plainly for the reason
`docs/adr/0011` states its own. A merge that must keep the keys the batch does not
mention has to know what they are, so it reads the whole table: peak memory is the
table plus the batch, where the watermarked path pays one partition plus the batch. The
tables that take this path are the small ones, but that is a fact about the callers
rather than a property the function enforces, and a large partitioned table merged this
way would materialise whole. The change that would fix it - merging partition by
partition and treating the untouched ones as kept by definition - is available and is
not made here, because nothing needs it and an unused optimisation is a second code
path to keep correct.

Five existing tests asserted the behaviour being removed - that a full reload drops a
row the source no longer has, that a replacement does not resurrect a removed row,
that a reload drops the periods it no longer has, that a reload with no rows leaves no
periods behind, and that a partitioned table reloaded with nothing keeps its
directory. They were correct tests of a decision that has been reversed here, and they
are rewritten rather than deleted, so that the new behaviour is asserted at the same
places the old one was.
