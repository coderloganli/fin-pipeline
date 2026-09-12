"""Judging a period: what gets flagged, what does not, and where the flags land.

Cases 15, 16 and 18-30 of task.md.

docs/adr/0050 puts `anomaly_flag` in a schema of its own, written after the mart has
been published, because a model that fails must not be able to stop the mart being
published. docs/adr/0051 trains on the mart rather than the landing layer, so the model
only ever sees figures that passed all six gates. docs/adr/0054 is what CI gates here:
behaviour, plus the anomalies the generator planted.

The cases that need a mart use the suite's own schemas; the two that only need the
flagging rule hand `judge` predictions directly, so they assert the rule rather than a
fit.
"""

from decimal import Decimal

import pytest

from conftest import (
    PLANTED_PERIOD,
    TEST_ANOMALY_SCHEMA,
    drop_schemas_under,
    flags_in,
    schema_for,
)
from ml import judge, store


def prediction(actual, lower, upper, *, median=None, period="2026-06",
               account="660204", centre="CC01"):
    """One row's worth of model output, without fitting anything."""
    return judge.Prediction(
        account_code=account,
        cost_center_code=centre,
        accounting_period=period,
        actual=float(actual),
        predicted=float(median if median is not None else (lower + upper) / 2),
        lower_bound=float(lower),
        upper_bound=float(upper),
    )


# --- case 15 ----------------------------------------------------------------

def test_a_crossed_interval_is_counted_and_is_not_reordered():
    """Case 15. Quantile fits made independently sometimes cross, and the row is where
    the fit is least trustworthy. Reordering the bounds would silently turn the least
    trustworthy row into a flag; counting it says so instead. See docs/adr/0052."""
    outcome = judge.decide([prediction(5000, lower=900, upper=100)])

    assert outcome.flags == []
    assert outcome.intervals_crossed == 1


# --- case 16 ----------------------------------------------------------------

def test_a_zero_width_interval_is_the_same_case_and_does_not_divide_by_zero():
    """Case 16. `score` normalises the excess by the interval width, so coinciding
    bounds would divide by zero. A fit with no dispersion to offer is not a basis on
    which to put a row in an analyst's queue."""
    outcome = judge.decide([prediction(5000, lower=700, upper=700)])

    assert outcome.flags == []
    assert outcome.intervals_crossed == 1


# --- the mart these cases judge ---------------------------------------------

@pytest.fixture
def judged(request, db, mart, ml_staging):
    """A built mart, an anomaly schema of this test's own, and a way to judge into it."""
    anomaly = schema_for(request.node.nodeid, TEST_ANOMALY_SCHEMA)
    built = mart(ml_staging)
    assert built.ok, f"the mart this case judges did not build:\n{built.output}"

    def run(periods, *, run_id="ml-test-run", family="quantile_linear"):
        return judge.run(
            connection=db,
            mart_schema=built.mart,
            anomaly_schema=anomaly,
            periods=periods,
            run_id=run_id,
            family=family,
        )

    run.built = built
    run.anomaly = anomaly
    yield run

    drop_schemas_under(db, anomaly)


# --- cases 18 and 19 --------------------------------------------------------

@pytest.mark.db
def test_every_flagged_row_is_outside_its_own_interval(judged, db):
    """Case 18. The definition of a flag, asserted over a whole judged period rather
    than on a constructed row."""
    judged(["2026-06"])

    rows = flags_in(db, judged.anomaly)
    assert rows, "judging a period produced no flags at all"
    for row in rows:
        assert (row["actual"] < row["lower_bound"]
                or row["actual"] > row["upper_bound"]), row


@pytest.mark.db
def test_nothing_inside_its_interval_is_flagged(judged, db):
    """Case 19. The other half, and the one that would let a broken model fill the
    analyst's queue. Compared row by row against everything the period held, not
    sampled."""
    detail = judged(["2026-06"])

    flagged = {
        (row["account_code"], row["cost_center_code"], row["accounting_period"])
        for row in flags_in(db, judged.anomaly)
    }
    inside = {
        (p.account_code, p.cost_center_code, p.accounting_period)
        for p in detail.predictions
        if p.lower_bound <= p.actual <= p.upper_bound
    }
    assert inside, "the period held no rows inside their interval"
    assert flagged & inside == set()


# --- case 20 ----------------------------------------------------------------

@pytest.mark.db
def test_a_flag_row_is_complete_and_names_the_run_that_wrote_it(judged, db):
    """Case 20. A figure that cannot name what produced it is what docs/product.md
    calls a lineage defect. `run_id` is checked for equality with the run that was
    handed in, not merely for being present - a hard-coded string would pass that."""
    judged(["2026-06"], run_id="20260911-abc123")

    rows = flags_in(db, judged.anomaly)
    assert rows
    for row in rows:
        for column, value in row.items():
            assert value is not None, f"{column} is null on {row}"
        assert row["run_id"] == "20260911-abc123"
        assert row["model_family"] == "quantile_linear"
        assert float(row["nominal_coverage"]) == pytest.approx(0.90)


# --- case 21 ----------------------------------------------------------------

def test_the_flag_records_which_side_was_crossed():
    """Case 21. The interval is two-sided: an expense account that stopped posting is
    as much a candidate for review as one that doubled, and an analyst needs to know
    which before opening the row."""
    outcome = judge.decide([
        prediction(50, lower=100, upper=900, account="660204"),
        prediction(5000, lower=100, upper=900, account="660104"),
    ])

    sides = {flag.account_code: flag.side for flag in outcome.flags}
    assert sides == {"660204": "below", "660104": "above"}


# --- cases 22, 23 and 24 ----------------------------------------------------

@pytest.mark.db
def test_the_flags_land_outside_the_mart(judged, db):
    """Case 22. In a schema of its own, and not in the mart - which docs/adr/0048
    drops and renames wholesale on every build."""
    judged(["2026-06"])

    with db.cursor() as cursor:
        cursor.execute(
            "SELECT table_schema FROM information_schema.tables "
            "WHERE table_name = 'anomaly_flag' AND table_schema IN (%s, %s)",
            (judged.anomaly, judged.built.mart),
        )
        schemas = {row[0] for row in cursor.fetchall()}
    assert schemas == {judged.anomaly}


@pytest.mark.db
def test_rebuilding_the_mart_does_not_take_the_flags_with_it(judged, db, mart,
                                                            ml_staging):
    """Case 23. The concrete form of docs/adr/0050's first argument: a promotion drops
    the mart schema and renames a build into place, and anything of ours inside it
    would go with it."""
    judged(["2026-06"])
    before = flags_in(db, judged.anomaly)
    assert before

    rebuilt = mart(ml_staging)
    assert rebuilt.ok, rebuilt.output

    assert flags_in(db, judged.anomaly) == before


@pytest.mark.db
def test_the_drift_gate_does_not_count_the_flags(judged, db):
    """Case 24. Not an exemption - `anomaly_flag` is not a dbt model and is not in the
    graph `model_row_count` walks. A row count that is supposed to move would fire the
    drift gate every month it worked correctly. See docs/adr/0036 and 0050."""
    judged(["2026-06"])

    with db.cursor() as cursor:
        cursor.execute(f'SELECT DISTINCT model FROM "{judged.built.mart}".model_row_count')
        counted = {row[0] for row in cursor.fetchall()}
    assert counted, "the mart recorded no model row counts at all"
    assert "anomaly_flag" not in counted


# --- case 25 ----------------------------------------------------------------

@pytest.mark.db
def test_rejudging_a_period_removes_a_flag_that_is_no_longer_anomalous(judged, db):
    """Case 25. Not "the row count did not double": a row flagged last night and
    inside its interval tonight has to leave the queue. Left behind it is a queue entry
    for a figure that is no longer anomalous, which costs an analyst more than a
    missing one - somebody investigates it.

    The mart row is moved onto its own prediction, so the second judgement finds it
    comfortably inside the interval it was outside of.
    """
    detail = judged(["2026-06"])
    flags = flags_in(db, judged.anomaly)
    assert flags, "nothing was flagged, so nothing can be shown to leave"
    target = flags[0]

    with db.cursor() as cursor:
        cursor.execute(
            f'UPDATE "{judged.built.mart}".agg_monthly_balance '
            f'SET balance_as_restated = %s '
            f'WHERE account_code = %s AND cost_center_code = %s '
            f'AND accounting_period = %s',
            (Decimal(str(target["predicted"])).quantize(Decimal("0.01")),
             target["account_code"], target["cost_center_code"],
             target["accounting_period"]),
        )
    judged(["2026-06"])

    keys = {(r["account_code"], r["cost_center_code"], r["accounting_period"])
            for r in flags_in(db, judged.anomaly)}
    assert (target["account_code"], target["cost_center_code"],
            target["accounting_period"]) not in keys


# --- case 26 ----------------------------------------------------------------

@pytest.mark.db
def test_judging_the_same_mart_twice_gives_the_same_flags(judged, db):
    """Case 26. Everything but `run_id`, which a real rerun necessarily changes - and
    the replacement rows carry the second run's id, which is what makes this consistent
    with case 20 rather than in tension with it."""
    judged(["2026-06"], run_id="run-one")
    first = flags_in(db, judged.anomaly)
    assert first

    judged(["2026-06"], run_id="run-two")
    second = flags_in(db, judged.anomaly)

    def without_run(rows):
        return [{k: v for k, v in row.items() if k != "run_id"} for row in rows]

    assert without_run(second) == without_run(first)
    assert {row["run_id"] for row in second} == {"run-two"}


# --- cases 27 and 28 --------------------------------------------------------

@pytest.mark.db
def test_judging_without_a_run_id_is_refused(judged, db):
    """Case 27. The same refusal `dbt-build` makes in pipeline/steps.py: a flag that
    cannot name its run is a lineage defect, and inventing an id would write one no
    record mentions."""
    with pytest.raises(judge.MissingRunId):
        judged(["2026-06"], run_id=None)


@pytest.mark.db
def test_a_missing_mart_is_reported_with_the_command_that_builds_it(db):
    """Case 28. The convention transform/db.py set for a missing database: the failure
    says what to do, rather than handing the caller the driver's error."""
    with pytest.raises(store.MartUnavailable) as failure:
        judge.run(
            connection=db,
            mart_schema="mart_test_definitely_not_built",
            anomaly_schema="anomaly_test_unused",
            periods=["2026-06"],
            run_id="ml-test-run",
            family="quantile_linear",
        )
    assert "pipeline" in str(failure.value)


# --- cases 29 and 30: the anomalies the generator planted -------------------
#
# Both shapes are planted on the first cost centre - `centres[0]` in generator/entries.py
# - so the whole key is assertable. Asserting the account alone would pass on a flag for
# the same account in some other cost centre, which is a different row and not the one
# that was planted.

PLANTED_CENTRE = "CC-001"

@pytest.fixture
def planted(request, db, mart, planted_staging):
    anomaly = schema_for(request.node.nodeid, TEST_ANOMALY_SCHEMA)
    built = mart(planted_staging)
    assert built.ok, f"the planted mart did not build:\n{built.output}"

    def run(periods, *, family="quantile_linear"):
        judge.run(
            connection=db, mart_schema=built.mart, anomaly_schema=anomaly,
            periods=periods, run_id="planted-run", family=family,
        )
        return flags_in(db, anomaly)

    yield run
    drop_schemas_under(db, anomaly)


@pytest.mark.db
def test_the_planted_concentrated_rise_reaches_the_queue(planted):
    """Case 29. The one gate here that is about effect rather than behaviour, and it is
    defensible because the answer is constructed rather than judged: the generator put
    the rise there, so a model that misses it has stopped working. See docs/adr/0054."""
    from generator.dimensions import GROWTH_DEBIT_ACCOUNT

    flagged = {(r["account_code"], r["cost_center_code"], r["accounting_period"])
               for r in planted([PLANTED_PERIOD])}

    assert (GROWTH_DEBIT_ACCOUNT, PLANTED_CENTRE, PLANTED_PERIOD) in flagged, (
        f"the planted rise on {GROWTH_DEBIT_ACCOUNT} / {PLANTED_CENTRE} in "
        f"{PLANTED_PERIOD} was not flagged; the queue holds {sorted(flagged)}"
    )


@pytest.mark.db
def test_the_planted_long_tail_reaches_the_queue(planted):
    """Case 30. The shape sorting by amount cannot find: a flat entry count against
    rising amounts. It has to reach the queue by the same route as the concentrated
    one, because the whole comparison downstream depends on both being there."""
    from generator.dimensions import LONG_TAIL_DEBIT_ACCOUNT

    flagged = {(r["account_code"], r["cost_center_code"], r["accounting_period"])
               for r in planted([PLANTED_PERIOD])}

    assert (LONG_TAIL_DEBIT_ACCOUNT, PLANTED_CENTRE, PLANTED_PERIOD) in flagged, (
        f"the planted long tail on {LONG_TAIL_DEBIT_ACCOUNT} / {PLANTED_CENTRE} in "
        f"{PLANTED_PERIOD} was not flagged; the queue holds {sorted(flagged)}"
    )


# --- a period the model cannot judge ----------------------------------------

@pytest.mark.db
def test_a_period_with_no_history_is_reported_rather_than_emptied(judged, db):
    """A period before the model has twelve periods of history produces no prediction.
    Replacing its flags anyway would delete what stood against it and write nothing
    back - emptying the analyst's queue for that period while the step reported
    `flagged: 0`, which reads as "judged and found clean".

    The two are different facts and an operator has to be able to tell them apart, so
    the period is reported as not judged and its flags are left where they are.
    """
    judged(["2026-06"])
    before = flags_in(db, judged.anomaly)
    assert before

    detail = judged(["2024-01"])          # the first period of the generated range

    assert detail.periods == []
    assert detail.not_judged == ["2024-01"]
    assert flags_in(db, judged.anomaly) == before


# --- the command line -------------------------------------------------------
#
# Both entry points are how a person reaches this layer without a pipeline run, and
# `config.json`'s `try` names them. Neither was exercised by anything above.

@pytest.mark.db
def test_the_judge_command_judges_a_period_and_says_what_it_did(judged, db, capsys,
                                                                monkeypatch):
    """`python -m ml.judge --periods 2026-06 --run-id <id>`. Resolving the schemas,
    opening the connection and closing it are all only reachable this way."""
    from transform import db as db_module

    resolved = dict(db_module.settings())          # before the patch, or it recurses
    resolved.update(POSTGRES_MART_SCHEMA=judged.built.mart,
                    POSTGRES_ANOMALY_SCHEMA=judged.anomaly)
    monkeypatch.setattr(db_module, "settings", lambda *a, **k: dict(resolved))

    assert judge.main(["--periods", "2026-06", "--run-id", "cli-run"]) == 0

    printed = capsys.readouterr().out
    assert "flagged" in printed
    rows = flags_in(db, judged.anomaly)
    assert rows and {row["run_id"] for row in rows} == {"cli-run"}


@pytest.mark.db
def test_the_backtest_command_writes_the_artefact(judged, tmp_path, capsys, monkeypatch):
    """`python -m ml.backtest`. Acceptance item 2 - both arms with comparable numbers
    from a time-series split - is satisfied by the file this writes, so the command
    that writes it is part of the deliverable rather than a convenience."""
    import json

    from ml import backtest
    from transform import db as db_module

    resolved = dict(db_module.settings())
    resolved.update(POSTGRES_MART_SCHEMA=judged.built.mart)
    monkeypatch.setattr(db_module, "settings", lambda *a, **k: dict(resolved))
    out = tmp_path / "backtest.json"

    assert backtest.main(["--splits", "2", "--out", str(out)]) == 0

    artefact = json.loads(out.read_text(encoding="utf-8"))
    assert artefact["nominal_coverage"] == 0.90
    for name in ("quantile_linear", "gradient_boosting"):
        assert len(artefact["arms"][name]["folds"]) == 2
    assert "written to" in capsys.readouterr().out


# --- the figure in the queue is the mart's own ------------------------------

def test_the_flag_reports_the_mart_figure_exactly_not_a_float_round_trip():
    """`actual` is what an analyst reads against a report built from Decimal
    (docs/adr/0013 and 0031), so it has to be the mart's own number. The value here is
    chosen to survive no float: `float(Decimal("12345678901234.57"))` is a different
    number, and a round-trip through it would put a figure in the queue that disagrees
    with the report by cents."""
    exact = Decimal("123456789012345678.91")
    assert Decimal(str(float(exact))) != exact, "pick a value a float cannot carry"

    outcome = judge.decide([
        judge.Prediction(
            account_code="660204", cost_center_code="CC-001",
            accounting_period="2026-06",
            actual=float(exact), predicted=100.0,
            lower_bound=50.0, upper_bound=150.0,
            actual_exact=exact,
        )
    ])

    assert len(outcome.flags) == 1
    assert outcome.flags[0].actual == exact
