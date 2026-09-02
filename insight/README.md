# insight

Turns a flagged balance into an explanation that can be checked.

The prompt receives the flagged row, its top contributing entries, period-over-period movement, and any related adjusting entries. The response must cite the source entry identifiers it relied on; a response without citations is rejected and retried.

Each stored explanation records the prompt version, the model build, the pipeline run
it was derived from, and — once the investigation loop replaces the fixed slice — the
trajectory it took: every step's tool, arguments, and result summary.

What that buys is a weaker guarantee than it looks. Given a run identifier the
trajectory can be replayed; the model's behaviour cannot be. And the replay is only
worth anything because the run beneath it is idempotent — if a rerun moved the numbers
in the mart, the same investigation would reach a different answer.

Evaluation runs against a fixed golden set: whether the explanation identifies the correct driver, whether it cites the correct entries, and whether it states anything false. The score is a CI gate.
