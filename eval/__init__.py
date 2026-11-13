"""Evaluation: the data an assertion can be made against, and the truth about it.

`eval.generator` writes the synthetic ledger the whole platform runs on. It lives here
rather than at the top level because the reason the data is synthetic is that the
answers have to be knowable: every failure mode is planted on purpose, so a test can
assert the pipeline responded correctly and a score can be given for whether an
anomaly was found and explained.

That is the only thing the two halves of this package share. `eval.generator` knows
nothing downstream of the six CSV files it writes - not the database, not Spark, not
the model - and `ingest/contracts/` is written by hand rather than derived from it,
so that a contract can catch the producer changing. See
docs/adr/0008-contracts-are-written-by-hand.md.
"""
