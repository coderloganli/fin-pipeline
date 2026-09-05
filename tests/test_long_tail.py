"""The long-tail anomaly: the same money spread thinly rather than concentrated.

This is the shape that separates an insight layer handed a fixed slice of top-N
entries from one that can ask its own questions. Both find a concentrated anomaly;
only this one distinguishes them. That is the whole reason it exists, so test case 2
asserts the two shapes are actually distinguishable in the data rather than assuming
it.
"""

import csv
import subprocess
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import pytest

from generator import Config, generate
from generator.dimensions import (
    GROWTH_CREDIT_ACCOUNT,
    GROWTH_DEBIT_ACCOUNT,
    LONG_TAIL_CREDIT_ACCOUNT,
    LONG_TAIL_DEBIT_ACCOUNT,
)
from generator.streams import LONG_TAIL, stream_for

# Imported as a bare module name, the way conftest is, because pytest puts the tests
# directory on sys.path. `from tests.test_generator import ...` additionally needs the
# working directory there, which `python -m pytest` provides and a bare `pytest` does
# not — a difference that passed locally and failed in CI.
from test_generator import (  # the predicates are defined once and reused
    TABLES,
    amount,
    has_amount_outliers,
    has_cost_centre_move,
    has_growing_account,
    has_late_entries,
    has_restatements,
    has_schema_drift,
    has_unbalanced_vouchers,
    median,
    raw_bytes,
    rows,
    run,
)

LONG_TAIL_VOUCHERS = 300
UPLIFT = Decimal("1.5")     # the predicate's floor; the generator plants 1.6
TOP_N = 20
TOP_N_SHARE = Decimal("0.10")
OUTLIER_MULTIPLE = 20


def totals_by_period(out: Path, account: str) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = defaultdict(Decimal)
    for row in rows(out, "gl_entry"):
        if row["account_code"] == account:
            totals[row["accounting_date"][:7]] += amount(row)
    return dict(totals)


def counts_by_period(out: Path, account: str) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows(out, "gl_entry"):
        if row["account_code"] == account:
            counts[row["accounting_date"][:7]] += 1
    return dict(counts)


def has_long_tail(out: Path, account: str = LONG_TAIL_DEBIT_ACCOUNT) -> bool:
    """A period whose total rose sharply while its largest twenty entries account
    for under a tenth of the rise.

    The `increase > 0` clause is not decoration: without it an account with no rise
    at all satisfies "under ten per cent", and the predicate would hold for data that
    has no anomaly in it.
    """
    totals = totals_by_period(out, account)
    if len(totals) < 2:
        return False

    for period, total in totals.items():
        others = [value for key, value in totals.items() if key != period]
        baseline = sum(others) / len(others)
        increase = total - baseline
        if increase <= 0 or total < baseline * UPLIFT:
            continue

        amounts = sorted(
            (amount(row) for row in rows(out, "gl_entry")
             if row["account_code"] == account and row["accounting_date"][:7] == period),
            reverse=True,
        )
        if total > 0 and sum(amounts[:TOP_N]) / total < TOP_N_SHARE:
            return True
    return False


def has_outlier_on(out: Path, account: str) -> bool:
    amounts = [
        amount(row) for row in rows(out, "gl_entry") if row["account_code"] == account
    ]
    if not amounts or median(amounts) <= 0:
        return False
    return max(amounts) >= median(amounts) * OUTLIER_MULTIPLE


def outlier_accounts(out: Path) -> set[str]:
    return {
        row["account_code"] for row in rows(out, "gl_entry")
        if row["entry_id"].startswith("X-OUTLIER")
    }


# --- Case 1 ----------------------------------------------------------------

def test_the_long_tail_switch_plants_a_long_tail(tmp_path):
    assert has_long_tail(run(tmp_path, long_tail_anomaly=True))
    assert not has_long_tail(run(tmp_path))


# --- Case 2 ----------------------------------------------------------------

def test_the_two_shapes_are_distinguishable(tmp_path):
    """Both crossings, not just one. If a concentrated anomaly also satisfied the
    long-tail predicate, the comparison this exists for would prove nothing."""
    out = run(tmp_path, long_tail_anomaly=True, amount_outliers=True)

    concentrated = outlier_accounts(out)
    assert concentrated, "amount_outliers planted nothing to contrast against"

    assert has_long_tail(out, LONG_TAIL_DEBIT_ACCOUNT)
    assert not has_outlier_on(out, LONG_TAIL_DEBIT_ACCOUNT), (
        "the long-tail account also looks concentrated; the shapes are not separable"
    )

    for account in concentrated:
        assert has_outlier_on(out, account)
        assert not has_long_tail(out, account), (
            f"the concentrated account {account} also satisfies the long-tail "
            f"predicate; the shapes are not separable"
        )


# --- Case 3 ----------------------------------------------------------------

def test_three_anomaly_switches_coexist(tmp_path):
    clean = run(tmp_path)
    out = run(tmp_path, long_tail_anomaly=True, growing_account=True, amount_outliers=True)

    assert has_long_tail(out)
    assert has_growing_account(out)
    assert has_amount_outliers(out)

    reserved = {
        LONG_TAIL_DEBIT_ACCOUNT, LONG_TAIL_CREDIT_ACCOUNT,
        GROWTH_DEBIT_ACCOUNT, GROWTH_CREDIT_ACCOUNT,
    }

    def untouched(source: Path) -> list[dict[str, str]]:
        return [
            row for row in rows(source, "gl_entry")
            if row["entry_id"].startswith("E-") and row["account_code"] not in reserved
        ]

    assert untouched(clean) == untouched(out), (
        "the three switches disturbed entries none of them owns"
    )


# --- Case 4 ----------------------------------------------------------------

def test_the_entry_count_is_three_hundred_and_does_not_move(tmp_path):
    """Not "the counts match on and off" — 0 == 0 satisfies that, and would pass
    against a generator that plants nothing at all."""
    for switches in ({}, {"long_tail_anomaly": True}):
        out = run(tmp_path, **switches)
        for account in (LONG_TAIL_DEBIT_ACCOUNT, LONG_TAIL_CREDIT_ACCOUNT):
            counts = counts_by_period(out, account)
            assert counts, f"{account} has no entries with switches {switches}"
            assert set(counts.values()) == {LONG_TAIL_VOUCHERS}, (
                f"{account} should carry exactly {LONG_TAIL_VOUCHERS} entries in every "
                f"period, got {sorted(set(counts.values()))}"
            )


# --- Case 5 ----------------------------------------------------------------

def test_the_switch_touches_only_its_own_accounts(tmp_path):
    clean = run(tmp_path)
    dirty = run(tmp_path, long_tail_anomaly=True)

    for table in TABLES:
        if table == "gl_entry":
            continue
        assert raw_bytes(clean, table) == raw_bytes(dirty, table), (
            f"long_tail_anomaly changed {table}, which it does not own"
        )

    owned = {LONG_TAIL_DEBIT_ACCOUNT, LONG_TAIL_CREDIT_ACCOUNT}

    def others(source: Path) -> list[dict[str, str]]:
        return [row for row in rows(source, "gl_entry") if row["account_code"] not in owned]

    assert others(clean) == others(dirty), (
        "long_tail_anomaly changed entries outside its own accounts"
    )

    # And it did change what it owns. Without this the test passes trivially against
    # a generator that plants nothing: everything outside an empty set is unchanged.
    def owned_rows(source: Path) -> list[dict[str, str]]:
        return [row for row in rows(source, "gl_entry") if row["account_code"] in owned]

    assert owned_rows(clean), "the long-tail accounts carry no entries at all"
    assert owned_rows(clean) != owned_rows(dirty), (
        "the switch left its own accounts unchanged, so it did nothing"
    )


def test_the_long_tail_stream_is_stable_across_processes():
    first, second = stream_for(42, LONG_TAIL), stream_for(42, LONG_TAIL)
    assert [first.random() for _ in range(5)] == [second.random() for _ in range(5)]

    program = (
        "from generator.streams import LONG_TAIL, stream_for;"
        "s = stream_for(42, LONG_TAIL);"
        "print([s.random() for _ in range(5)])"
    )
    outputs = {
        subprocess.run(
            [sys.executable, "-c", program],
            capture_output=True, text=True, check=True,
            env={"PYTHONHASHSEED": seed, "PATH": ""},
            cwd=Path(__file__).resolve().parent.parent,
        ).stdout.strip()
        for seed in ("0", "1", "12345")
    }
    assert len(outputs) == 1, f"long_tail stream varies with PYTHONHASHSEED: {outputs}"


# --- Case 6 ----------------------------------------------------------------

def test_the_reserved_accounts_are_declared_and_every_reference_resolves(tmp_path):
    clean = run(tmp_path)
    dirty = run(tmp_path, long_tail_anomaly=True)

    declared = {row["account_code"] for row in rows(clean, "dim_account_src")}
    for account in (LONG_TAIL_DEBIT_ACCOUNT, LONG_TAIL_CREDIT_ACCOUNT):
        assert account in declared, f"{account} is not in dim_account_src"

    assert raw_bytes(clean, "dim_account_src") == raw_bytes(dirty, "dim_account_src")

    for row in rows(dirty, "gl_entry"):
        assert row["account_code"] in declared, (
            f"{row['entry_id']} posts to {row['account_code']}, which is not declared"
        )


# --- Case 7 ----------------------------------------------------------------

def test_the_clean_baseline_gains_long_tail_rows_without_becoming_dirty(tmp_path):
    """The long-tail rows are unconditional, so they enter every run there will ever
    be. If they tripped an existing predicate, every later ticket would start from a
    baseline that is quietly not clean."""
    out = run(tmp_path)

    # They are present — without this the rest of the test passes against a
    # generator that produces nothing.
    counts = counts_by_period(out, LONG_TAIL_DEBIT_ACCOUNT)
    assert counts and set(counts.values()) == {LONG_TAIL_VOUCHERS}

    per_doc: dict[str, tuple[Decimal, Decimal]] = defaultdict(lambda: (Decimal(0), Decimal(0)))
    for row in rows(out, "gl_entry"):
        if row["entry_id"].startswith("L-"):
            dr, cr = per_doc[row["doc_id"]]
            per_doc[row["doc_id"]] = (dr + Decimal(row["amount_dr"]), cr + Decimal(row["amount_cr"]))
    assert per_doc, "no long-tail vouchers found under the L- prefix"
    assert all(dr == cr for dr, cr in per_doc.values()), "long-tail vouchers do not balance"

    assert not has_long_tail(out)
    assert not has_late_entries(out)
    assert not has_restatements(out)
    assert not has_cost_centre_move(out)
    assert not any(has_schema_drift(out, table) for table in TABLES)
    assert not has_unbalanced_vouchers(out)
    assert not has_growing_account(out)
    assert not has_amount_outliers(out)


# --- Added at stage 8 -------------------------------------------------------

def _long_tail_rows(out: Path) -> dict[str, dict[str, str]]:
    return {row["entry_id"]: row for row in rows(out, "gl_entry") if row["entry_id"].startswith("L-")}


def test_only_the_target_period_moves_and_it_moves_by_exactly_the_uplift(tmp_path):
    """Case 1 only required a 1.5x rise, which is the detection threshold, not the
    planted value. Nothing pinned the uplift itself, and nothing stopped the switch
    from raising every period rather than one."""
    from generator.entries import LONG_TAIL_UPLIFT

    clean, dirty = _long_tail_rows(run(tmp_path)), _long_tail_rows(run(tmp_path, long_tail_anomaly=True))
    assert clean and clean.keys() == dirty.keys(), "the switch changed which entries exist"

    changed_periods, unchanged = set(), 0
    for entry_id, before in clean.items():
        after = dirty[entry_id]
        if before == after:
            unchanged += 1
            continue
        changed_periods.add(before["accounting_date"][:7])
        assert amount(after) == (amount(before) * LONG_TAIL_UPLIFT).quantize(Decimal("0.01")), (
            f"{entry_id} moved by {amount(after) / amount(before):.3f}x, not {LONG_TAIL_UPLIFT}x"
        )

    assert len(changed_periods) == 1, f"more than one period moved: {sorted(changed_periods)}"
    assert unchanged, "every long-tail entry changed; the switch is not period-specific"


def test_the_long_tail_vouchers_balance_with_the_switch_on(tmp_path):
    """Raising only the debit side would satisfy almost every other assertion here
    while quietly producing unbalanced data."""
    out = run(tmp_path, long_tail_anomaly=True)
    assert not has_unbalanced_vouchers(out)

    per_doc: dict[str, tuple[Decimal, Decimal]] = defaultdict(lambda: (Decimal(0), Decimal(0)))
    for row in rows(out, "gl_entry"):
        if row["entry_id"].startswith("L-"):
            dr, cr = per_doc[row["doc_id"]]
            per_doc[row["doc_id"]] = (dr + Decimal(row["amount_dr"]), cr + Decimal(row["amount_cr"]))
    assert per_doc
    assert all(dr == cr for dr, cr in per_doc.values())


def test_the_command_line_exposes_the_switch(tmp_path):
    from generator.__main__ import main

    out = tmp_path / "cli"
    main(["--seed", "42", "--out", str(out), "--enable-long-tail-anomaly"])
    assert has_long_tail(out), "--enable-long-tail-anomaly did not reach the config"
    assert not has_amount_outliers(out), "a switch that was not asked for was turned on"


def test_a_single_period_is_refused(tmp_path):
    """With no other period there is nothing to compare against, so the switch would
    be on and the anomaly undetectable — the same failure growing_account refuses."""
    with pytest.raises(ValueError, match="long_tail_anomaly needs at least"):
        generate(Config(
            seed=42, out_dir=tmp_path / "single",
            periods="2026-01:2026-01", long_tail_anomaly=True,
        ))


# --- the two shapes, seen through the vendor ---------------------------------
#
# Cases 18, 19, 19a and 20 of task.md. This is what the whole ticket is for: the
# insight layer's `breakdown_by_vendor` has to be able to reject "is it one
# supplier?" for a long tail and confirm it for a concentrated rise. If both shapes
# looked the same under that question, the third step's A/B comparison would have
# nothing to show. See docs/adr/0022.

from generator import dimensions as dim_module            # noqa: E402

VENDOR_SPREAD_MIN = 20              # a long tail has to reach this many suppliers
LONG_TAIL_TOP_VENDOR_SHARE = Decimal("0.15")
CONCENTRATED_TOP_VENDOR_SHARE = Decimal("0.95")


def increase_by_vendor(clean: Path, dirty: Path, account: str) -> dict[str, Decimal]:
    """How much each vendor's total on `account` grew when the switch was turned on.

    A difference rather than a snapshot, and that is the point. The long-tail account
    carries its three hundred vouchers whether or not the switch is on
    (docs/adr/0007), so grouping only the switched-on run by vendor looks spread out
    even when the uplift never happened.
    """
    def totals(out: Path) -> dict[str, Decimal]:
        found: dict[str, Decimal] = defaultdict(Decimal)
        for row in rows(out, "gl_entry"):
            if row["account_code"] == account and row["vendor_code"]:
                found[row["vendor_code"]] += amount(row)
        return found

    before, after = totals(clean), totals(dirty)
    return {
        vendor: after[vendor] - before.get(vendor, Decimal(0))
        for vendor in after
        if after[vendor] - before.get(vendor, Decimal(0)) > 0
    }


def top_share(increase: dict[str, Decimal]) -> Decimal:
    total = sum(increase.values())
    assert total > 0, "the switch produced no increase at all"
    return max(increase.values()) / total


def test_the_long_tail_increase_is_spread_across_many_vendors(tmp_path):
    """Case 18."""
    clean = run(tmp_path)
    dirty = run(tmp_path, long_tail_anomaly=True)

    increase = increase_by_vendor(clean, dirty, LONG_TAIL_DEBIT_ACCOUNT)

    assert len(increase) >= VENDOR_SPREAD_MIN, (
        f"the long tail reached only {len(increase)} vendors"
    )
    assert top_share(increase) < LONG_TAIL_TOP_VENDOR_SHARE, (
        f"the largest vendor took {top_share(increase):.1%} of the increase"
    )


def test_the_growth_increase_lands_on_one_vendor(tmp_path):
    """Case 19. One vendor rather than two: the measure is the top vendor's share of
    the increase, and a pair splitting it evenly would sit near a half, which is
    neither concentrated nor spread."""
    clean = run(tmp_path)
    dirty = run(tmp_path, growing_account=True)

    increase = increase_by_vendor(clean, dirty, GROWTH_DEBIT_ACCOUNT)

    assert top_share(increase) > CONCENTRATED_TOP_VENDOR_SHARE, (
        f"the largest vendor took only {top_share(increase):.1%} of the increase"
    )


def test_the_outlier_increase_lands_on_one_vendor(tmp_path):
    """Case 19a. The other concentrated shape. Without it, amount_outliers could keep
    spreading vendors at random and no test would say so."""
    clean = run(tmp_path)
    dirty = run(tmp_path, amount_outliers=True)

    account = next(
        row["account_code"] for row in rows(dirty, "gl_entry")
        if row["entry_id"].startswith("X-OUTLIER") and Decimal(row["amount_dr"]) > 0
    )
    increase = increase_by_vendor(clean, dirty, account)

    assert top_share(increase) > CONCENTRATED_TOP_VENDOR_SHARE, (
        f"the largest vendor took only {top_share(increase):.1%} of the increase"
    )


def test_one_measure_separates_the_two_shapes(tmp_path):
    """Case 20. The same question, asked of both: is the increase concentrated in one
    supplier? The concentrated shapes say yes and the long tail says no, which is
    exactly the step that lets an investigation reject its first hypothesis and go
    looking for a second one."""
    clean = run(tmp_path)

    concentrated = top_share(increase_by_vendor(
        clean, run(tmp_path, growing_account=True), GROWTH_DEBIT_ACCOUNT))
    spread = top_share(increase_by_vendor(
        clean, run(tmp_path, long_tail_anomaly=True), LONG_TAIL_DEBIT_ACCOUNT))

    assert concentrated > CONCENTRATED_TOP_VENDOR_SHARE
    assert spread < LONG_TAIL_TOP_VENDOR_SHARE
    assert concentrated > spread * 5, (concentrated, spread)


def test_the_clean_long_tail_amounts_stay_inside_their_band(tmp_path):
    """The Top-20 arithmetic in docs/adr/0007 is computed from this band and from three
    hundred vouchers a period. Nothing pinned the band itself, so widening it would
    only have been caught if the sampled seed happened to break the predicate."""
    out = run(tmp_path)
    amounts = [
        amount(row) for row in rows(out, "gl_entry")
        if row["account_code"] == LONG_TAIL_DEBIT_ACCOUNT
    ]
    assert amounts
    assert min(amounts) >= Decimal("250.00"), min(amounts)
    assert max(amounts) <= Decimal("350.00"), max(amounts)
