"""General-ledger entries and adjustments.

This is the table that reaches tens of millions of rows, so everything here yields.
Nothing accumulates: a voucher's lines are built together, balanced together, handed
over and forgotten.

Two conventions make the switches independent of each other, which is what test case
11 checks:

- Planted rows carry their own id prefix and are appended after the ordinary ones,
  so turning a switch on never shifts an ordinary entry's id or position.
- Each switch draws from its own random stream, so it never moves another's numbers.
"""

import re
from datetime import date, timedelta
from decimal import Decimal

from . import schema
from .dimensions import (
    BASE_CURRENCY,
    CURRENCIES,
    GROWTH_CREDIT_ACCOUNT,
    GROWTH_DEBIT_ACCOUNT,
    LONG_TAIL_CREDIT_ACCOUNT,
    LONG_TAIL_DEBIT_ACCOUNT,
    RESERVED_ACCOUNTS,
)
from .streams import (
    AMOUNT_OUTLIERS,
    ADJUSTMENTS,
    ENTRIES,
    GROWING_ACCOUNT,
    LATE_ENTRIES,
    LONG_TAIL,
    RESTATEMENTS,
    UNBALANCED_VOUCHERS,
    stream_for,
)

# Ordinary amounts sit in a narrow band. Monthly totals per account are then stable
# enough that ordinary data never accidentally satisfies the growth predicate, which
# would make test case 6's negative half flaky.
MIN_CENTS = 10_000
MAX_CENTS = 50_000

GROWTH_START_CENTS = 200_000
GROWTH_FACTOR = Decimal("1.8")   # comfortably above the 1.5 the predicate looks for
GROWTH_MONTHS = 4                # four totals give three month-on-month steps

OUTLIER_MULTIPLE = 30            # the predicate looks for 20x the median

# The long tail. Its amounts sit in a narrower band than the ordinary entries, and
# that is what makes the acceptance threshold reachable: under a uniform uplift the
# top twenty's share of the increase equals their share of the total, so the worst
# case is computable rather than something to sample.
#
#     20 x 350 / (20 x 350 + 280 x 250) =  9.09%   under the 10% threshold, always
#     20 x 500 / (20 x 500 + 280 x 100) = 26.32%   the ordinary band would never pass
#
# It is also what the shape means: an account with a long tail is one full of similar
# small claims. An account whose amounts vary fivefold has no tail to speak of.
LONG_TAIL_VOUCHERS = 300
LONG_TAIL_MIN_CENTS = 25_000
LONG_TAIL_MAX_CENTS = 35_000
LONG_TAIL_UPLIFT = Decimal("1.6")   # the predicate looks for 1.5


def _cents(value: int) -> Decimal:
    return (Decimal(value) / 100).quantize(schema.AMOUNT_PLACES)


def _voucher(
    entry_id_debit: str,
    entry_id_credit: str,
    doc_id: str,
    accounting_date: date,
    posted_at: date,
    debit_account: str,
    credit_account: str,
    cost_centre: str,
    currency: str,
    debit: Decimal,
    credit: Decimal | None = None,
) -> list[dict[str, object]]:
    """Two lines sharing a doc_id. `credit` differs from `debit` only when a caller
    is deliberately producing an unbalanced voucher."""
    credit = debit if credit is None else credit
    common = {
        "version": 1,
        "accounting_date": schema.format_date(accounting_date),
        "posted_at": schema.format_date(posted_at),
        "cost_center_code": cost_centre,
        "currency": currency,
        "doc_id": doc_id,
    }
    return [
        {
            **common,
            "entry_id": entry_id_debit,
            "account_code": debit_account,
            "amount_dr": schema.format_amount(debit),
            "amount_cr": schema.format_amount(Decimal(0)),
        },
        {
            **common,
            "entry_id": entry_id_credit,
            "account_code": credit_account,
            "amount_dr": schema.format_amount(Decimal(0)),
            "amount_cr": schema.format_amount(credit),
        },
    ]


def months_in(periods: str) -> list[date]:
    """Parse `YYYY-MM:YYYY-MM` strictly.

    A malformed or reversed range used to produce an empty month list, and the
    failure surfaced much later as a ZeroDivisionError somewhere unrelated.
    """
    match = re.fullmatch(r"(\d{4})-(\d{2}):(\d{4})-(\d{2})", periods.strip())
    if not match:
        # Matched on the whole string, not sliced out of it: slicing accepted
        # "2026-01-extra:2026-02" while claiming to be strict.
        raise ValueError(f"periods must look like YYYY-MM:YYYY-MM, got {periods!r}")
    try:
        first = date(int(match.group(1)), int(match.group(2)), 1)
        last = date(int(match.group(3)), int(match.group(4)), 1)
    except ValueError as failure:
        raise ValueError(f"periods has a month outside 1-12: {periods!r}") from failure
    if last < first:
        raise ValueError(f"periods runs backwards: {periods!r}")
    out, current = [], first
    while current <= last:
        out.append(current)
        current = (current + timedelta(days=32)).replace(day=1)
    return out


def ordinary(config, months: list[date], accounts: list[str], centres: list[str]):
    """The clean ledger: balanced vouchers, posted within days of being booked.

    Accounts are assigned round-robin rather than at random so each one receives a
    similar number of lines every month. Random assignment would make monthly totals
    jump around, and a jump of fifty per cent three months running is exactly what
    the growth predicate looks for — the negative half of test case 6 would then fail
    for no reason.
    """
    rng = stream_for(config.seed, ENTRIES)
    vouchers_per_period = max(1, config.entries_per_period // 2)
    counter = 0

    for month in months:
        days = (schema.period_close(month) - month).days + 1
        for index in range(vouchers_per_period):
            booked = month + timedelta(days=rng.randrange(days))
            yield from _voucher(
                entry_id_debit=f"E-{counter:09d}",
                entry_id_credit=f"E-{counter + 1:09d}",
                doc_id=f"D-{counter // 2:09d}",
                accounting_date=booked,
                posted_at=booked + timedelta(days=rng.randrange(4)),
                debit_account=accounts[index % len(accounts)],
                credit_account=accounts[(index + 1) % len(accounts)],
                cost_centre=centres[index % len(centres)],
                currency=CURRENCIES[index % len(CURRENCIES)],
                debit=_cents(rng.randrange(MIN_CENTS, MAX_CENTS)),
            )
            counter += 2


def planted(config, months: list[date], accounts: list[str], centres: list[str]):
    """Everything the switches add. Ids carry an `X-` prefix so the ordinary
    sequence above is untouched whichever switches are on."""

    if config.late_entries:
        rng = stream_for(config.seed, LATE_ENTRIES)
        for index, month in enumerate(months[: max(1, len(months) // 2)]):
            booked = month + timedelta(days=rng.randrange(5))
            # Measured from the close of the period, not from the booking date: an
            # entry posted three days late is ordinary, one posted a month after its
            # period closed is not.
            yield from _voucher(
                entry_id_debit=f"X-LATE-{index:06d}-D",
                entry_id_credit=f"X-LATE-{index:06d}-C",
                doc_id=f"X-LATE-{index:06d}",
                accounting_date=booked,
                posted_at=schema.period_close(booked) + timedelta(days=45),
                debit_account=accounts[index % len(accounts)],
                credit_account=accounts[(index + 1) % len(accounts)],
                cost_centre=centres[index % len(centres)],
                currency=BASE_CURRENCY,
                debit=_cents(rng.randrange(MIN_CENTS, MAX_CENTS)),
            )

    if config.unbalanced_vouchers:
        rng = stream_for(config.seed, UNBALANCED_VOUCHERS)
        debit = _cents(rng.randrange(MIN_CENTS, MAX_CENTS))
        yield from _voucher(
            entry_id_debit="X-UNBAL-000000-D",
            entry_id_credit="X-UNBAL-000000-C",
            doc_id="X-UNBAL-000000",
            accounting_date=months[0] + timedelta(days=3),
            posted_at=months[0] + timedelta(days=4),
            debit_account=accounts[0],
            credit_account=accounts[1],
            cost_centre=centres[0],
            currency=BASE_CURRENCY,
            debit=debit,
            credit=debit + _cents(rng.randrange(1_000, 5_000)),
        )

    if config.growing_account:
        if len(months) < GROWTH_MONTHS:
            # Otherwise the switch is on and quietly does nothing: three
            # month-on-month increases need four monthly totals.
            raise ValueError(
                f"growing_account needs at least {GROWTH_MONTHS} periods, "
                f"got {len(months)} from {config.periods!r}"
            )
        # Constructed, not discovered. Finding a growing account by scanning would
        # mean holding every row, which is what streaming exists to avoid.
        stream_for(config.seed, GROWING_ACCOUNT)
        amount_cents = GROWTH_START_CENTS
        for index, month in enumerate(months[:GROWTH_MONTHS]):
            yield from _voucher(
                entry_id_debit=f"X-GROW-{index:06d}-D",
                entry_id_credit=f"X-GROW-{index:06d}-C",
                doc_id=f"X-GROW-{index:06d}",
                accounting_date=month + timedelta(days=10),
                posted_at=month + timedelta(days=11),
                debit_account=GROWTH_DEBIT_ACCOUNT,
                credit_account=GROWTH_CREDIT_ACCOUNT,
                cost_centre=centres[0],
                currency=BASE_CURRENCY,
                debit=_cents(amount_cents),
            )
            amount_cents = int(amount_cents * GROWTH_FACTOR)

    if config.amount_outliers:
        rng = stream_for(config.seed, AMOUNT_OUTLIERS)
        typical = (MIN_CENTS + MAX_CENTS) // 2
        yield from _voucher(
            entry_id_debit="X-OUTLIER-000000-D",
            entry_id_credit="X-OUTLIER-000000-C",
            doc_id="X-OUTLIER-000000",
            accounting_date=months[len(months) // 2] + timedelta(days=rng.randrange(5)),
            posted_at=months[len(months) // 2] + timedelta(days=6),
            debit_account=accounts[0],
            credit_account=accounts[1],
            cost_centre=centres[0],
            currency=BASE_CURRENCY,
            debit=_cents(typical * OUTLIER_MULTIPLE),
        )


def adjustments(config, months: list[date], accounts: list[str], centres: list[str], entry_count: int):
    """Corrections always; restatements only when the switch is on.

    The entries they point at are named rather than remembered: ids are formulaic and
    the count is known up front, so referencing them costs no memory.
    """
    rng = stream_for(config.seed, ADJUSTMENTS)
    restatement_rng = stream_for(config.seed, RESTATEMENTS)

    def row(index: int, kind: str, target: int, month: date, drawn) -> dict[str, object]:
        booked = month + timedelta(days=drawn.randrange(20))
        return {
            "entry_id": f"A-{kind[:4].upper()}-{index:06d}",
            "version": 2,
            "accounting_date": schema.format_date(booked),
            "posted_at": schema.format_date(schema.period_close(booked) + timedelta(days=20)),
            "account_code": accounts[index % len(accounts)],
            "cost_center_code": centres[index % len(centres)],
            "currency": BASE_CURRENCY,
            "amount_dr": schema.format_amount(_cents(drawn.randrange(MIN_CENTS, MAX_CENTS))),
            "amount_cr": schema.format_amount(Decimal(0)),
            "doc_id": f"A-{kind[:4].upper()}-{index:06d}",
            "adjusts_entry_id": f"E-{target:09d}",
            "adjustment_type": kind,
        }

    for index in range(3):
        yield row(index, "correction", rng.randrange(entry_count), months[index % len(months)], rng)

    if config.restatements:
        for index in range(2):
            yield row(
                index,
                "restatement",
                restatement_rng.randrange(entry_count),
                months[index % len(months)],
                restatement_rng,
            )


def long_tail(config, months: list[date], centres: list[str]):
    """The long-tail account's entries, present whether or not the switch is on.

    The switch multiplies one period's amounts; it does not add rows. A long-tail
    anomaly that changed the entry count would be the wrong shape — a flat count
    against rising amounts is exactly how this is told apart from simply doing more
    business, and it is the observation the insight layer is meant to make.

    Ids carry an `L-` prefix rather than `X-`: `X-` marks rows that exist only while
    a switch is on, and these exist always.
    """
    if config.long_tail_anomaly and len(months) < 2:
        # With no other period there is nothing to compare the raised one against,
        # so the switch would be on and the anomaly undetectable.
        raise ValueError(
            f"long_tail_anomaly needs at least 2 periods, got {len(months)} "
            f"from {config.periods!r}"
        )

    rng = stream_for(config.seed, LONG_TAIL)
    target = months[len(months) // 2]

    for period_index, month in enumerate(months):
        uplift = (
            LONG_TAIL_UPLIFT
            if config.long_tail_anomaly and month == target
            else Decimal(1)
        )
        days = (schema.period_close(month) - month).days + 1
        for index in range(LONG_TAIL_VOUCHERS):
            booked = month + timedelta(days=rng.randrange(days))
            # +1 because randrange's upper bound is exclusive; the band is
            # inclusive of 350, which is the value the 9.09% bound assumes.
            base = _cents(rng.randrange(LONG_TAIL_MIN_CENTS, LONG_TAIL_MAX_CENTS + 1))
            yield from _voucher(
                entry_id_debit=f"L-{period_index:03d}-{index:06d}-D",
                entry_id_credit=f"L-{period_index:03d}-{index:06d}-C",
                doc_id=f"L-{period_index:03d}-{index:06d}",
                accounting_date=booked,
                posted_at=booked + timedelta(days=rng.randrange(4)),
                debit_account=LONG_TAIL_DEBIT_ACCOUNT,
                credit_account=LONG_TAIL_CREDIT_ACCOUNT,
                cost_centre=centres[index % len(centres)],
                currency=BASE_CURRENCY,
                debit=(base * uplift).quantize(schema.AMOUNT_PLACES),
            )
