# The anomaly layer gates behaviour and the anomalies it was given, not its score

summary: CI fails on leakage, on a row flagged from inside its interval, on an
incomplete flag row, on an irreproducible rerun, and on a planted anomaly that was
missed; coverage, interval width and pinball loss are written down and not thresholded.

## Context

Every capability in this repository is accepted on what its gate can stop, not on the
gate existing. `tests/test_mart_gates.py` builds a failure for each of the six mart
gates and asserts the named test node goes red. The anomaly layer has to meet the same
standard, and the obvious candidate — assert the model scores above some number — is a
gate that fails at random.

Model quality moves for reasons that are not defects. A regularisation path lands
differently, the generator's seed changes, a period is added. A CI job that goes red on
a pinball loss drifting from 0.83 to 0.87 teaches people to rerun it, and a gate people
rerun is worse than no gate.

`docs/产品文档.md` has already ruled on the specific numbers anyone would reach for. The
200–400 flagged rows, the 8–15 minutes per row, the team of four: it marks them as
placeholder design decisions rather than measurements, and says they must be measured
before they are used as CI thresholds.

## Decision

**CI gates behaviour.** Each of these is a test that can be made to fail by breaking the
thing it names:

- **No leakage.** For every fold the splitter yields, the maximum training period is
  strictly less than the minimum test period. Checked on the folds themselves.
- **Inside the interval is never flagged.** A row whose actual value lies between its
  own lower and upper bound produces no flag. Asserted over a whole judged period, not
  on a constructed row.
- **A flag row is complete.** Prediction, lower bound, upper bound, residual, score,
  side, `run_id`, model identification. A flag that cannot say what produced it is the
  same defect `docs/product.md` calls a lineage failure.
- **A rerun reproduces.** The same mart and the same seed give the same flags. Fitting
  is seeded and the fit is deterministic, so this is a real property rather than an
  aspiration.
- **A crossed or zero-width interval does not flag.** Where the 0.05 fit lands at or
  above the 0.95 fit (`docs/adr/0052`), the row is counted in `intervals_crossed` and
  produces no flag.
- **A re-judged period replaces its flags.** A row flagged by one run and inside its
  interval on the next is gone from the table, not left behind.

**And one gate on effect: the planted anomalies are caught.** The generator plants two
shapes on reserved accounts — `growing_account` concentrates a rise into large entries,
`long_tail_anomaly` raises amounts across many small ones on a dedicated account with
the entry count held flat (`docs/adr/0007`, `docs/adr/0022`). A ledger generated with
either switch on, judged over the planted period, must flag the (account, cost centre)
rows carrying the planted rise. The answer is constructed, so the gate needs no
person — which is the property `开发计划.md` names as what makes this layer gateable at
all.

**Scores are recorded, not thresholded.** Realised coverage against the nominal 90%,
mean interval width, `mean_pinball_loss` per quantile, MAE of the median fit, and the
number of rows flagged per period, for both arms, written as a backtest artefact a
person reads. No test asserts a bound on any of them.

## Reasoning

**The distinction is between what the platform promises and what the model achieves.**
The platform promises that a flag means a value fell outside a fitted interval, that the
interval was fitted without seeing the future, that the flag says where it came from,
and that the same inputs give the same flags. Those are properties of the code and they
either hold or they do not. How well a quantile regression fits a synthetic ledger is
not a promise, and `docs/product.md` puts it out of scope in as many words: what is
being built is how a data platform feeds and evaluates a model, not the model.

**The planted-anomaly gate is the exception that proves the rule, and it is defensible
precisely because it is not a score.** It does not assert a level of performance; it
asserts that a rise deliberately put into the data reaches the queue. If that stops
holding, either the model has been broken or the generator has, and both are worth a red
run. It is also the only gate here that would catch the layer degrading into something
that flags plausibly but meaninglessly.

**Rejected: a coverage floor.** Asserting realised coverage stays above, say, 80% of
nominal catches an interval fitted too narrow, and it is tempting because it looks like
a property rather than a score. It is not: the number would be picked by looking at what
the current fit produces, which is the hand-picked threshold this whole ticket exists to
avoid. Recording coverage every run and letting a person read the trend does the same
work honestly.

**Rejected: a count bound on the queue.** Flagging between N and M rows per period is
the product's 200–400 turned into a test, and `docs/产品文档.md` explicitly forbids that
until the figure is measured.

## Consequences

**The backtest artefact is a deliverable, and a command writes it.**
`python -m ml.backtest` scores both arms over the mart and writes JSON to
`data/ml/backtest.json`. That is what acceptance item 2 — ridge and gradient boosting
both have comparable backtest numbers — is satisfied by: a file somebody opens, rather
than a number printed during a CI run and gone by the time anyone asks.

**Comparing "the mart is unchanged" means excluding `model_row_count`.** A test that
fails `judge` on purpose and then asserts the mart was left alone has to compare the
reported models, not the schema wholesale: every successful build appends a row to
`mart.model_row_count` by construction (`docs/adr/0036`), and `docs/adr/0038` already
excludes that table from the reproducibility criterion for the same reason. The gate is
that the reported figures did not move, not that the mart schema is byte-identical.

**A model that gets worse can pass CI.** That is deliberate and it is the trade being
made: the planted-anomaly gate catches a model that has stopped working, and a model
that has merely got worse shows up in the recorded numbers, where a person decides
whether it matters. The alternative buys earlier detection with a gate that cries wolf.

**When the product's figures are eventually measured rather than assumed, this decision
is the one to revisit.** A measured expectation for queue size is a legitimate gate; the
placeholder is not.
