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
python -m generator --seed 42 --periods 2026-01:2026-12 \
    --entries-per-period 100000 --out data/source
```

Each failure mode has a flag — `--enable-late-entries`, `--enable-restatements`,
`--enable-cost-centre-move`, `--enable-unbalanced-vouchers`,
`--enable-growing-account`, `--enable-amount-outliers`,
`--enable-long-tail-anomaly` — plus
`--schema-drift {none,add_column,drop_column}` with `--schema-drift-table`.

With no flags the output is the clean baseline: every voucher balances, nothing is
late, no series is anomalous, and every cost centre has one row. That baseline is
what the pipeline tests start from, so it is asserted directly rather than assumed.

The ledger reads like one. The chart of accounts follows the accounting standard's
numbering two levels deep — a four-digit first-level account, a six-digit detail account
whose first four digits are its parent — and code, name and type agree with each other.
An ordinary voucher is either an invoice (debit an expense detail, credit the payable
matching its category, carry a supplier and a description) or a sale (debit receivables,
credit revenue, carry no supplier, because revenue is earned from customers). See
`docs/adr/0021` and `0022`.

`schema.py` is the single truth for what the generator emits — column names, order,
date format, decimal places. What ingest *expects* is declared separately under
`ingest/contracts/`; a contract derived from its producer could not catch the
producer changing.

## The two anomaly shapes

`--enable-amount-outliers` and `--enable-growing-account` are concentrated: the
increase sits in a few large entries, and sorting by amount finds it.

`--enable-long-tail-anomaly` is the opposite. Account `6996` carries three hundred
entries every period whether or not the switch is on; the switch raises one period's
amounts by 60%. The count stays flat, the largest entry is barely above the median,
and the largest twenty account for under a tenth of the increase — so sorting by
amount finds nothing, and the rise is only visible by asking a different question.

The pair exists so the insight layer's two implementations can be compared. Both find
a concentrated anomaly; only the long tail separates them.

The suppliers are shaped for that comparison too. The long-tail account's three hundred
vouchers rotate across thirty office-supply vendors, so its increase arrives spread
thinly; the growth switch posts to a single marketing vendor and the outlier switch to a
single vendor of its own account's category, so theirs arrives concentrated. An outlier
has to sit on an ordinary account - being twenty times the median of the entries around
it is what makes it one. Asked "is this one supplier?", the two shapes answer
differently — which is the step that lets an investigation reject its first hypothesis
and look for a second.
