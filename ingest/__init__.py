"""Landing source extracts into the raw layer.

What has landed: `contracts/`, which states what ingest expects each source table to
look like, and `validate.py`, which applies a contract to a source file and decides
whether the run continues. An added column is compatible and warns; a column that has
gone, a reordering, a value that no longer fits its declared type or rule, a repeated
primary key, or a broken row constraint fails the run.

The watermarked incremental load, the idempotent merge and the run record are this
package's remaining responsibilities and belong to their own tickets. Nothing here
imports the generator: a check derived from its producer cannot catch the producer
changing. See docs/adr/0008-contracts-are-written-by-hand.md.
"""
