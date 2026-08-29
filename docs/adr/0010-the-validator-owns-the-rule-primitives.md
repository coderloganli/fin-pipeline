# The rule primitives live in the validator, not in the contract loader

summary: `check_value`, `check_row_constraint` and the row walk move out of the tests into `ingest/validate.py`; the contract loader stays data-blind, and the tests import the production definitions rather than keeping a second copy.

## Context

The previous ticket wrote the five contracts and a loader that validates the contract
files themselves. To claim that the contracts agreed with the data the generator
produces, it needed something that could apply a contract to rows — so it wrote one,
in `tests/test_contracts.py`, and labelled it as not production code:

> Not production code: it is not exported and nothing outside this module uses it. It
> exists because "the contracts agree with what the generator produces" is this
> ticket's acceptance criterion.

The loader's own docstring names the successor: applying a contract to rows "belongs
to the validator, which is the next task. The rule primitives currently live in the
tests, and move here when that task needs them in production." This is that task.

## Decision

**The primitives move into `ingest/validate.py`.** Not into `ingest/contracts/`.
That package loads and checks contract files and states, in its first paragraph, that
it does not look at data; moving a row walker into it would make that statement false
and would leave the package doing two unrelated jobs.

**`tests/test_contracts.py` imports them from there** and keeps no copy. The two
definitions cannot then drift, which is the thing a copy guarantees eventually.

**The loader keeps what is genuinely shared.** `is_null` and `DATE_FORMAT` stay in
`ingest/contracts/` and the validator imports them. They are statements about what a
contract *means* — an empty field is null, a date is written `YYYY-MM-DD` — not
procedures for checking data, and the loader is where the meaning of a contract is
declared.

**The prohibition on importing the generator extends to the validator.** Nothing
under `ingest/` imports `generator`, and the existing AST test that enforces this for
`ingest` and `ingest.contracts` gains `ingest.validate`.

## Reasoning

The alternative was to leave the primitives in the tests and have the validator call
into them. That is backwards in a way that would have been discovered late: the test
suite would become a dependency of production code, the code would ship without the
functions it needs, and the packaging test that already exists for the contracts would
have to be extended to cover a test module, which is not a thing that gets packaged.

Moving them makes one previously invisible property visible. In the tests, these
functions returned a string or `None` and the caller turned that into an assertion.
In production the same return has to become a report a caller can act on differently
depending on severity, which is what ADR 0012 settles. That reshaping is the real
work of the move; the function bodies themselves transfer nearly unchanged, and
deliberately so — they already have parametrised tests covering `2026-1-5` passing
`strptime`, `NaN` raising on comparison, and the other paths that were found the hard
way. Rewriting them would throw that away for nothing.

**One body does change, and finding out why is the argument for moving them at all.**
`check_row_constraint` evaluates `exactly_one_nonzero` with `Decimal(row[name])`, which
raises on a value that is not a number. In the tests it never fired: it only ever ran
against generated data that had already satisfied the column rules, so the unparseable
value it would choke on could not reach it. In production it is reached by the first
case this ticket exists for — `amount_dr` holding `abc` — and the validator would crash
on exactly the input it is meant to report.

The fix is not a `try` around the call. A rule about the relationship between two
values cannot be evaluated when one of them is not a value, so the row walk records
which columns already produced a finding for that row and skips any constraint that
references one of them. The column finding is the diagnosis; a second message about the
constraint would be noise. `check_row_constraint` also returns a message rather than
raising if reached with something unparseable, so no caller can reproduce the crash by
using it directly.

`violations`, the tests' row walk, does not move. It takes a materialised list of rows,
which is the shape ADR 0011 forbids in production. The per-row half becomes `check_row`
and the accumulation around it belongs to `validate_table`. Its two callers in the
tests call `validate_source` instead, which is what production does with those files.
