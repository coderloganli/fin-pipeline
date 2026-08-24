# insight

Turns a flagged balance into an explanation that can be checked.

The prompt receives the flagged row, its top contributing entries, period-over-period movement, and any related adjusting entries. The response must cite the source entry identifiers it relied on; a response without citations is rejected and retried.

Each stored explanation records the prompt version, the model, and the pipeline run it was derived from, so the exact input context can be reconstructed later.

Evaluation runs against a fixed golden set: whether the explanation identifies the correct driver, whether it cites the correct entries, and whether it states anything false. The score is a CI gate.
