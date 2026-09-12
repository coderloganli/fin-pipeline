"""Turning predictions into flags, and writing them down.

A balance is flagged when it falls outside its own interval. That is the whole rule, and
nothing about it is a threshold somebody picked: the bounds come from the fit, and the
only number a person chose is the nominal coverage - a statement about how many rows are
expected outside, not about any amount. See docs/adr/0052.

Two kinds of row produce no flag and are counted instead. An interval whose bounds
crossed is not an interval, and one of zero width has no dispersion to offer and would
divide `score` by zero. Both mean the fit is least trustworthy exactly there, so
repairing them quietly would turn the worst-understood rows into queue entries.

    python -m ml.judge --periods 2026-06 --run-id <id>
"""

import argparse
from dataclasses import dataclass, field
from decimal import Decimal

from ml import features, models, store

__all__ = ["Prediction", "Flag", "Outcome", "Detail", "MissingRunId",
           "decide", "predict_periods", "run", "main"]


class MissingRunId(RuntimeError):
    """Judging without the run's identifier. The same refusal `dbt-build` makes.

    A flag that cannot name its run is the lineage defect `docs/product.md` calls a
    defect rather than a limitation, and inventing an id would write one no record
    mentions.
    """


@dataclass(frozen=True)
class Prediction:
    """One balance and what the model said about it."""

    account_code: str
    cost_center_code: str
    accounting_period: str
    actual: float
    predicted: float
    lower_bound: float
    upper_bound: float
    # The mart's own Decimal, when the caller has it. `actual` is a float because the
    # comparison against the bounds is one, but the figure written into an analyst's
    # queue should be the number the report shows rather than a round-trip of it.
    actual_exact: object = None


@dataclass(frozen=True)
class Flag:
    """One row of `anomaly_flag`. The columns are the table's, in its order."""

    account_code: str
    cost_center_code: str
    accounting_period: str
    actual: Decimal
    predicted: Decimal
    lower_bound: Decimal
    upper_bound: Decimal
    residual: Decimal
    score: Decimal
    side: str
    model_family: str
    nominal_coverage: Decimal
    run_id: str


@dataclass
class Outcome:
    flags: list = field(default_factory=list)
    intervals_crossed: int = 0


@dataclass
class Detail:
    """What the step hands back to the run record."""

    flagged: int = 0
    intervals_crossed: int = 0
    periods: list = field(default_factory=list)
    # Periods that were asked for and could not be judged: no history in front of them.
    # Reported rather than folded into `periods`, because "judged and found nothing" and
    # "could not judge" are different facts and an operator needs to tell them apart.
    not_judged: list = field(default_factory=list)
    predictions: list = field(default_factory=list)


PLACES = Decimal("0.0001")


def _money(value) -> Decimal:
    return Decimal(str(value)).quantize(PLACES)


def decide(predictions, *, family: str = "quantile_linear",
           run_id: str = "", nominal_coverage: float = models.NOMINAL_COVERAGE) -> Outcome:
    """The flagging rule, and nothing else.

    Separate from fitting so that it can be asserted on directly: the cases that matter
    most here - a crossed interval, a zero-width one, which side was crossed - are
    about this rule rather than about a fit, and a test that had to fit a model first
    would be asserting two things at once.
    """
    outcome = Outcome()
    for one in predictions:
        if one.upper_bound <= one.lower_bound:
            outcome.intervals_crossed += 1
            continue
        if one.lower_bound <= one.actual <= one.upper_bound:
            continue

        width = one.upper_bound - one.lower_bound
        excess = (one.actual - one.upper_bound if one.actual > one.upper_bound
                  else one.lower_bound - one.actual)
        outcome.flags.append(Flag(
            account_code=one.account_code,
            cost_center_code=one.cost_center_code,
            accounting_period=one.accounting_period,
            actual=(one.actual_exact if one.actual_exact is not None
                    else _money(one.actual)),
            predicted=_money(one.predicted),
            lower_bound=_money(one.lower_bound),
            upper_bound=_money(one.upper_bound),
            residual=_money(one.actual - one.predicted),
            score=_money(excess / width),
            side="above" if one.actual > one.upper_bound else "below",
            model_family=family,
            nominal_coverage=_money(nominal_coverage),
            run_id=run_id,
        ))
    return outcome


def predict_periods(balances, periods, *, family: str, random_state: int = 0):
    """One fit per judged period, on everything known before it.

    Per period rather than once for the whole set: judging February and March together
    off a single fit trained before February would predict March without February in it,
    although February is known by the time March is judged. That is the shape of the
    last backtest fold, and it is what docs/adr/0053 means by fitting on everything
    before the period being judged - so scoring and serving stay one code path rather
    than two that drift.

    Everything fitted is fitted on that period's training rows, the size band included
    (docs/adr/0055). A period with no history in front of it yields nothing, and the
    caller is told which those were rather than left to read an empty list as "clean".
    """
    balances = list(balances)
    out, judged_periods = [], []

    for period in sorted(set(periods)):
        prepared = features.for_period(balances, period)
        if prepared is None:
            continue
        design, train, judged = prepared

        X, y, exact = design.X, design.y, design.actual
        arm = models.build(family, random_state=random_state).fit(
            [X[i] for i in train], [y[i] for i in train]
        )
        prediction = arm.predict([X[i] for i in judged])

        judged_periods.append(period)
        out.extend(
            Prediction(
                account_code=design.rows[row].key[0],
                cost_center_code=design.rows[row].key[1],
                accounting_period=design.rows[row].key[2],
                actual=y[row],
                predicted=prediction.median[position],
                lower_bound=prediction.lower[position],
                upper_bound=prediction.upper[position],
                actual_exact=exact[row],
            )
            for position, row in enumerate(judged)
        )
    return out, judged_periods


def run(*, connection, mart_schema: str, anomaly_schema: str, periods,
        run_id: str | None, family: str = "quantile_linear",
        random_state: int = 0) -> Detail:
    """Judge these periods and replace their flags. What the `judge` step calls."""
    if not run_id:
        raise MissingRunId(
            "judging needs the run's run_id: every flag carries the run that produced "
            "it, which is how a reader gets back to the mart build it was computed "
            "against. Steps are run through pipeline.run.run_step, which sets it."
        )

    balances = store.read_balances(connection, mart_schema)
    predictions, judged = predict_periods(
        balances, periods, family=family, random_state=random_state
    )
    outcome = decide(predictions, family=family, run_id=run_id)

    # Only the periods that were judged. Replacing a period the model could not judge
    # would delete the flags standing against it and write nothing back - emptying the
    # analyst's queue for it while the step reported `flagged: 0`, which reads as
    # "judged and found clean". A period with less than a year of history in front of
    # it is the ordinary way to reach that, so it is not a rare path.
    store.replace_periods(
        connection, anomaly_schema, judged, outcome.flags, family=family
    )
    skipped = sorted(set(periods) - set(judged))
    return Detail(
        flagged=len(outcome.flags),
        intervals_crossed=outcome.intervals_crossed,
        periods=judged,
        not_judged=skipped,
        predictions=predictions,
    )


def main(argv=None) -> int:
    from transform import db

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--periods", nargs="+", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--family", default="quantile_linear", choices=models.ARMS)
    args = parser.parse_args(argv)

    values = db.settings()
    with db.connection_from(values) as connection:
        detail = run(
            connection=connection,
            mart_schema=values["POSTGRES_MART_SCHEMA"],
            anomaly_schema=values["POSTGRES_ANOMALY_SCHEMA"],
            periods=args.periods,
            run_id=args.run_id,
            family=args.family,
        )
    print(f"{detail.flagged} flagged over {', '.join(detail.periods) or 'nothing'}"
          f" ({detail.intervals_crossed} intervals crossed)")
    if detail.not_judged:
        print(f"not judged, no history in front of them: "
              f"{', '.join(detail.not_judged)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
