# generator

Synthetic general-ledger data. The generator is a deliverable in its own right: the tests and the demo both depend on it producing specific failure modes on demand.

It must be able to emit, under explicit switches:

- entries that arrive after their accounting period has closed
- restatements as distinct from corrections
- an organisational change that moves a cost centre mid-year
- a source table that gains a column, and one that loses a column
- unbalanced vouchers where debits do not equal credits
- an account whose balance grows abnormally for three consecutive months
- individual outlier amounts

Because the anomalies are planted deliberately, the golden set used to evaluate the LLM layer has known correct answers.
