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
whole. The raw layer holds text and nothing but the declared columns.

The run record - a run identifier, the watermark range, row counts and duration, and
that identifier carried by every downstream artefact - is this package's remaining
responsibility and belongs to its own ticket.

Nothing here imports the generator: a check derived from its producer cannot catch the
producer changing. See docs/adr/0008-contracts-are-written-by-hand.md.
"""
