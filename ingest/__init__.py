"""Landing source extracts into the raw layer.

`contracts/` states what ingest expects each source table to look like, and
`validate.py` applies a contract to a source file and decides whether the run
continues. An added column is compatible and warns; a column that has gone, a
reordering, a value that no longer fits its declared type or rule, a repeated primary
key, or a broken row constraint fails the run. A failure also names the dbt models the
change would break, which each contract makes possible by declaring the source it
feeds.

`raw.py` and `load.py` hold the incremental load. Each contract names the column its
table advances its watermark on, and a run reads from that watermark less an overlap
window, merges what it read into the accounting periods it touches, and moves the
watermark only once every partition is written. A table declaring no watermark is
replaced whole. The raw layer holds text: the columns the contract declares, and two
run identifiers - the run that first landed the key, and the run that last wrote the
file. Both reach the mart, as `source_first_run_id` and `source_last_run_id`.

`runs.py` is the run record. Every run appends a started and a finished event to
`data/raw/_state/runs.jsonl`, with a pair of events around each step in between,
carrying the run identifier, the watermark range, the row counts, the source file digest
and the duration. `python -m ingest.runs` reads them back. A run that died is reported
as having died, naming the step it died in.

`affected.py` records what a run dirtied - the accounting periods, and the dimension
versions it landed - and leaves interpreting that to `transform/`, which is the half
that knows which periods a version reaches.

Nothing here imports the generator: a check derived from its producer cannot catch the
producer changing. See docs/adr/0008-contracts-are-written-by-hand.md.
"""
