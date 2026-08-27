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

## Using it

```
python -m generator --seed 42 --periods 2026-01:2026-12                     --entries-per-period 100000 --out data/source
```

Each failure mode has a flag — `--enable-late-entries`, `--enable-restatements`,
`--enable-cost-centre-move`, `--enable-unbalanced-vouchers`,
`--enable-growing-account`, `--enable-amount-outliers` — plus
`--schema-drift {none,add_column,drop_column}` with `--schema-drift-table`.

With no flags the output is the clean baseline: every voucher balances, nothing is
late, no series is anomalous, and every cost centre has one row. That baseline is
what the pipeline tests start from, so it is asserted directly rather than assumed.

`schema.py` is the single truth for what the generator emits — column names, order,
date format, decimal places. What ingest *expects* is declared separately under
`ingest/contracts/`; a contract derived from its producer could not catch the
producer changing.
