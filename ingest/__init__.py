"""Landing source extracts into the raw layer.

What has landed: `contracts/`, which states what ingest expects each source table to
look like, and `validate.py`, which applies a contract to a source file and decides
whether the run continues. An added column is compatible and warns; a column that has
gone, a reordering, a value that no longer fits its declared type or rule, a repeated
primary key, or a broken row constraint fails the run.

`raw.py` and `load.py` have landed too: each contract names the column its table
advances its watermark on, and a run reads from that watermark less an overlap window,
merges what it read into the accounting periods it touches, and moves the watermark
only once every partition is written. A table declaring no watermark is replaced
whole. The raw layer holds text: the columns the contract declares, and two run
identifiers.

`runs.py` has landed: every load appends a started and a finished event to
`data/raw/_state/runs.jsonl`, carrying the run identifier, the watermark range, the row
counts, the source file digest and the duration, and `python -m ingest.runs` reads them
back. Every raw row carries the run that first landed it and the run that last wrote
it. Carrying that identifier into artefacts beyond the raw layer waits for a layer
beyond the raw one to exist.

Nothing here imports the generator: a check derived from its producer cannot catch the
producer changing. See docs/adr/0008-contracts-are-written-by-hand.md.
"""
