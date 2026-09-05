# The chart of accounts follows the accounting standard, two levels deep

## Context

The generator's accounts were `6100` through `6139`, named `account 6100`, with
`account_type` assigned round-robin: `ACCOUNT_TYPES[index % 5]`. Structurally that is
enough — nothing downstream reads the name, and the type is only ever compared against
the contract's allowed set.

It stops being enough the moment a person reads a row. `6100` typed as an asset is not
a defensible ledger: under the Chinese accounting standard the `6xxx` range is profit
and loss, and a chart whose codes contradict their own types is one an accountant
dismisses on sight. The insight layer makes this worse rather than better: an
explanation that says an anomaly sits in "account 6100" explains nothing, and a golden
set built on such data cannot be judged by a human reader.

There is a second, quieter problem. `parent_code` is the contract's only nullable
column, and its nullability is justified in the contract's own comment as "empty for a
top-level account". With a synthetic hierarchy — every tenth code being its own decade's
parent — that justification was true by construction rather than by meaning.

## Decision

The chart is modelled on the standard's account numbering, two levels deep:

- **First-level accounts** carry a four-digit code and an empty `parent_code`:
  `1001` Cash on hand (asset), `1122` Accounts receivable (asset), `2202` Accounts
  payable (liability), `4001` Paid-in capital (equity), `6001` Revenue from main
  operations (revenue), `6601` Selling expenses (expense), and so on across all five
  types.
- **Detail accounts** carry a six-digit code whose first four digits are their parent:
  `660101` Selling expenses - travel, `660201` Administrative expenses - office,
  `220201` Accounts payable - materials.

Names are English like everything else committed here; what follows the standard is the
numbering, which is the part that carries meaning.

Code, name and `account_type` agree with one another. The hierarchy is exactly two
levels: a row with a non-empty `parent_code` names a row whose `parent_code` is empty.

The four accounts reserved for anomalies move into this scheme as detail accounts and
keep the property `docs/adr/0007` gave them — they are emitted whether or not their
switch is on:

| Anomaly | Debit | Credit |
|---|---|---|
| Long tail | `660204` Administrative expenses - office supplies | `220204` Accounts payable - office supplies |
| Growth | `660104` Selling expenses - marketing campaigns | `220205` Accounts payable - marketing |

An invoice is credited to the payable that matches its expense's category — travel to
`220203` Accounts payable - travel agency, office to `220206` — rather than to whichever
payable a rotation happened to reach. A travel expense owed to a materials supplier is
the kind of detail that costs nothing to get right.

Cost centres and departments get real names on the same grounds.

## Reasoning

The acceptance criterion for this ticket is that a person can read one row and
understand what happened. Names alone would satisfy the letter of that and not its
intent: an expense posted to an account typed as equity reads as nonsense to the only
audience that matters here.

Two levels rather than one because `parent_code` should mean something. A detail
account under a first-level account is what a real chart looks like, it gives the
point-in-time join a hierarchy worth rolling up, and it makes the nullable column
nullable for a reason a reader can check.

Two levels rather than three or four because depth costs test surface and buys nothing
this project needs. A rollup that works for two works for n; the code does not become
more honest by being deeper.

The reserved accounts stay reserved. It is tempting, now that the chart is realistic,
to plant the anomalies on ordinary accounts — but `docs/adr/0007` reserved them so that
a planted increase never leaks into an ordinary account's monthly totals, and that
argument is unaffected by what the accounts are called.

The cost is that account codes change, so the tests that assert on them change with
them. They assert through named constants rather than literals, so the change is
mechanical; the alternative — keeping codes that contradict their types — is a defect
that would be found by the first person who read the data.
