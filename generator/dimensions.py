"""Accounts, cost centres and exchange rates.

These are the small tables — hundreds of rows — so they are built in memory. The
entries are not; see `entries.py`.
"""

from datetime import date, timedelta
from decimal import Decimal

from . import schema
from .streams import COST_CENTRE_MOVE, DIMENSIONS, VENDORS, stream_for

BASE_CURRENCY = "CNY"
CURRENCIES = ("CNY", "EUR", "USD", "GBP")

# A rate is drawn at the resolution it is written at - an integer number of
# millionths - and stays integral until one exact division. No float participates,
# for the reason amounts draw whole cents: a float has no exact decimal value, and
# once the pipeline multiplies amounts by rates, where the rounding happened becomes
# a question somebody has to answer. See docs/adr/0013-a-rate-is-not-an-amount.md.
RATE_MIN_MICROS = 5_000_000   # 5.000000
RATE_MAX_MICROS = 9_000_000   # 9.000000, exclusive
DRIFT_PPM = 20_000            # the daily jitter, +/- 2 per cent in parts per million

ACCOUNT_TYPES = ("asset", "liability", "equity", "revenue", "expense")

# The chart follows the accounting standard's numbering, two levels deep: a
# first-level account is four digits with no parent, a detail account is six digits
# whose first four are its parent. Code, name and type agree with one another, which
# the round-robin `ACCOUNT_TYPES[index % 5]` this replaces did not - it typed 6100, a
# profit-and-loss code, as an asset. See docs/adr/0021.
#
# (code, name, parent, account_type)
CHART: tuple[tuple[str, str, str, str], ...] = (
    ("1001", "Cash on hand", "", "asset"),
    ("1002", "Bank deposits", "", "asset"),
    ("100201", "Bank deposits - operating account", "1002", "asset"),
    ("100202", "Bank deposits - foreign currency", "1002", "asset"),
    ("1122", "Accounts receivable", "", "asset"),
    ("1123", "Prepayments", "", "asset"),
    ("1221", "Other receivables", "", "asset"),
    ("1403", "Raw materials", "", "asset"),
    ("1405", "Finished goods", "", "asset"),
    ("1601", "Fixed assets", "", "asset"),
    ("1602", "Accumulated depreciation", "", "asset"),

    ("2202", "Accounts payable", "", "liability"),
    ("220201", "Accounts payable - materials", "2202", "liability"),
    ("220202", "Accounts payable - services", "2202", "liability"),
    ("220203", "Accounts payable - travel agency", "2202", "liability"),
    ("220206", "Accounts payable - office", "2202", "liability"),
    ("220207", "Accounts payable - advertising", "2202", "liability"),
    ("220204", "Accounts payable - office supplies", "2202", "liability"),
    ("220205", "Accounts payable - marketing", "2202", "liability"),
    ("2211", "Employee benefits payable", "", "liability"),
    ("2221", "Taxes payable", "", "liability"),
    ("2241", "Other payables", "", "liability"),

    ("4001", "Paid-in capital", "", "equity"),
    ("4002", "Capital surplus", "", "equity"),
    ("4101", "Surplus reserve", "", "equity"),
    ("4103", "Profit for the year", "", "equity"),
    ("4104", "Profit distribution", "", "equity"),

    ("6001", "Revenue from main operations", "", "revenue"),
    ("600101", "Revenue - product sales", "6001", "revenue"),
    ("600102", "Revenue - services", "6001", "revenue"),
    ("6051", "Other operating revenue", "", "revenue"),
    ("6111", "Investment income", "", "revenue"),
    ("6301", "Non-operating income", "", "revenue"),

    ("6401", "Cost of main operations", "", "expense"),
    ("6403", "Taxes and surcharges", "", "expense"),
    ("6601", "Selling expenses", "", "expense"),
    ("660101", "Selling expenses - travel", "6601", "expense"),
    ("660102", "Selling expenses - entertainment", "6601", "expense"),
    ("660103", "Selling expenses - advertising", "6601", "expense"),
    ("660104", "Selling expenses - marketing campaigns", "6601", "expense"),
    ("6602", "Administrative expenses", "", "expense"),
    ("660201", "Administrative expenses - office", "6602", "expense"),
    ("660202", "Administrative expenses - payroll", "6602", "expense"),
    ("660203", "Administrative expenses - depreciation", "6602", "expense"),
    ("660204", "Administrative expenses - office supplies", "6602", "expense"),
    ("6603", "Finance costs", "", "expense"),
    ("6604", "Research and development expenses", "", "expense"),
    ("660401", "R&D expenses - materials", "6604", "expense"),
    ("660402", "R&D expenses - payroll", "6604", "expense"),
    ("6711", "Non-operating expenses", "", "expense"),
)

# The four pools an ordinary voucher is built from. A voucher is either an invoice -
# debit an expense detail, credit a payables detail, and carry a vendor - or a sale -
# debit receivables, credit a revenue detail, and carry none. See docs/adr/0022.
#
# Payroll and depreciation are in the chart but not in the expense pool: nobody
# invoices you for either, so a vendor on them would be a detail got wrong for free.
EXPENSE_DETAIL = ("660101", "660102", "660103", "660201", "660401")
PAYABLE_DETAIL = ("220201", "220202", "220203", "220206", "220207")
REVENUE_DETAIL = ("600101", "600102")
RECEIVABLE = "1122"

MATERIALS = "materials"
SERVICES = "services"
TRAVEL = "travel"
OFFICE = "office"
MARKETING = "marketing"

VENDOR_CATEGORIES = (MATERIALS, SERVICES, TRAVEL, OFFICE, MARKETING)

# Which kind of supplier an account is invoiced by. It decides both the vendor and the
# wording, so a travel expense reads like one.
CATEGORY_OF_ACCOUNT: dict[str, str] = {
    "660101": TRAVEL,
    "660102": SERVICES,
    "660103": MARKETING,
    "660201": OFFICE,
    "660401": MATERIALS,
}

# Which payable an invoice is credited to. Chosen by the expense account's category
# rather than by position, so a travel expense is not owed to a materials supplier.
PAYABLE_FOR_CATEGORY: dict[str, str] = {
    MATERIALS: "220201",
    SERVICES: "220202",
    TRAVEL: "220203",
    OFFICE: "220206",
    MARKETING: "220207",
}

# Phrases rather than free text: the generator is deterministic, and a description
# that varied between runs would break that for nothing. The sets are disjoint, so a
# description can be traced back to the category that produced it.
PHRASES: dict[str, tuple[str, ...]] = {
    MATERIALS: ("Raw material purchase", "Component restock", "Packaging materials"),
    SERVICES: ("Consulting services", "Client entertainment", "Professional fees"),
    TRAVEL: ("Travel reimbursement", "Airfare and lodging", "Client visit expenses"),
    OFFICE: ("Office supplies purchase", "Stationery restock", "Printer consumables"),
    MARKETING: ("Campaign media buy", "Trade show booth", "Advertising placement"),
}

# A sale has no supplier, but it still needs wording, and it should match what was
# sold rather than rotate independently of it.
SALE_PHRASES: dict[str, str] = {
    "600101": "Product sale",
    "600102": "Service engagement",
}

# How many suppliers each category has, and it is a design input rather than an
# accident: the long tail spreads across office supplies, and the concentrated
# anomalies land on one of the two marketing suppliers. Without the asymmetry the two
# anomaly shapes look identical under `breakdown_by_vendor`. See docs/adr/0022.
VENDORS_PER_CATEGORY: dict[str, int] = {
    OFFICE: 30,
    MATERIALS: 12,
    SERVICES: 10,
    TRAVEL: 6,
    MARKETING: 2,
}
VENDOR_COUNT = sum(VENDORS_PER_CATEGORY.values())

VENDOR_STEMS = (
    "Northwind", "Bluebird", "Cedar Ridge", "Harbourline", "Kestrel", "Lakeside",
    "Meridian", "Norwood", "Oakfield", "Pinehurst", "Quarry Lane", "Redstone",
    "Silverbrook", "Thornbury", "Underhill", "Valemount", "Westgate", "Yarrow",
    "Ashcombe", "Brightwater", "Copperfield", "Dunmore", "Eastfield", "Fernhill",
    "Glenmore", "Havenridge", "Ironwood", "Juniper", "Kingsley", "Longacre",
    "Marlowe", "Netherfield",
)

VENDOR_SUFFIX: dict[str, str] = {
    MATERIALS: "Materials",
    SERVICES: "Consulting",
    TRAVEL: "Travel",
    OFFICE: "Office Supplies",
    MARKETING: "Media",
}

DEPARTMENTS = ("DEPT-SALES", "DEPT-RND", "DEPT-OPS", "DEPT-ADMIN")

COST_CENTRE_NAMES = (
    "Sales - East", "Sales - North", "Sales - South",
    "R&D - platform", "R&D - devices", "R&D - tooling",
    "Manufacturing", "Logistics", "Customer support",
    "Finance", "People and workplace", "Executive office",
)

COST_CENTRE_COUNT = len(COST_CENTRE_NAMES)

# Reserved for the growth anomaly. Keeping the planted growth off the ordinary
# accounts stops it leaking into their monthly totals. They are declared in CHART
# above and emitted whether or not the switch is on, so turning it on does not change
# dim_account_src. See docs/adr/0007 and 0021.
GROWTH_DEBIT_ACCOUNT = "660104"      # Selling expenses - marketing campaigns
GROWTH_CREDIT_ACCOUNT = "220205"     # Accounts payable - marketing
# Reserved for the long-tail anomaly. Unlike the growth accounts these carry entries
# whether or not the switch is on: the anomaly raises their amounts, and a steady
# entry count is the shape's diagnostic feature.
LONG_TAIL_DEBIT_ACCOUNT = "660204"   # Administrative expenses - office supplies
LONG_TAIL_CREDIT_ACCOUNT = "220204"  # Accounts payable - office supplies

RESERVED_ACCOUNTS = (
    GROWTH_DEBIT_ACCOUNT,
    GROWTH_CREDIT_ACCOUNT,
    LONG_TAIL_DEBIT_ACCOUNT,
    LONG_TAIL_CREDIT_ACCOUNT,
)

# The reserved debit accounts are invoiced like any other expense detail, so they
# carry a category too.
CATEGORY_OF_ACCOUNT[LONG_TAIL_DEBIT_ACCOUNT] = OFFICE
CATEGORY_OF_ACCOUNT[GROWTH_DEBIT_ACCOUNT] = MARKETING

EPOCH = date(2020, 1, 1)


def accounts(seed: int) -> list[dict[str, object]]:
    """The chart of accounts, in code order.

    Read from CHART rather than computed, because a chart is a statement about a
    business and not a sequence. `seed` is kept in the signature so the call site does
    not have to know that this one no longer draws.
    """
    return [
        {
            "account_code": code,
            "name": name,
            "parent_code": parent,
            "account_type": account_type,
            "effective_date": schema.format_date(EPOCH),
        }
        for code, name, parent, account_type in sorted(CHART)
    ]


def vendors(seed: int) -> list[dict[str, object]]:
    """The suppliers, one row each, in code order.

    Names are assembled from a stem and the category's suffix, so they read like
    companies without a table of sixty literals. The stems are shuffled from the
    vendors stream: a stream of its own is what lets this table exist without moving
    any entry's amount or date. See docs/adr/0005 and 0022.
    """
    rng = stream_for(seed, VENDORS)
    stems = list(VENDOR_STEMS)
    rng.shuffle(stems)

    rows: list[dict[str, object]] = []
    number = 1
    for category in VENDOR_CATEGORIES:
        for index in range(VENDORS_PER_CATEGORY[category]):
            stem = stems[index % len(stems)]
            rows.append(
                {
                    "vendor_code": f"V-{number:04d}",
                    "name": f"{stem} {VENDOR_SUFFIX[category]}",
                    "category": category,
                }
            )
            number += 1
    return rows


def vendors_by_category(seed: int) -> dict[str, list[str]]:
    """Vendor codes grouped by category, in code order.

    The entry generator assigns a vendor by index within its account's category rather
    than by drawing one, so a voucher's supplier is a function of its position and of
    nothing else.
    """
    grouped: dict[str, list[str]] = {category: [] for category in VENDOR_CATEGORIES}
    for row in vendors(seed):
        grouped[row["category"]].append(row["vendor_code"])
    return grouped


def cost_centres(seed: int, move: bool) -> list[dict[str, object]]:
    """One row per cost centre, unless `move` is on.

    The move is what makes a point-in-time join necessary: a March transaction has to
    be reported against the department the cost centre belonged to in March, not the
    one it belongs to now.
    """
    rng = stream_for(seed, DIMENSIONS)
    rows = []
    for index in range(COST_CENTRE_COUNT):
        code = f"CC-{index + 1:03d}"
        rows.append(
            {
                "cc_code": code,
                "name": COST_CENTRE_NAMES[index % len(COST_CENTRE_NAMES)],
                "dept_code": DEPARTMENTS[index % len(DEPARTMENTS)],
                "effective_date": schema.format_date(EPOCH),
            }
        )
    rng.shuffle(rows)
    rows.sort(key=lambda row: row["cc_code"])

    if move:
        # A separate stream, so turning this on does not disturb anything above.
        move_rng = stream_for(seed, COST_CENTRE_MOVE)
        moved = rows[move_rng.randrange(len(rows))]
        successor = dict(moved)
        # Rotate within DEPARTMENTS by position. Reading the last character of the
        # code as a number worked while departments were numbered and raises the
        # moment one is called DEPT-SALES.
        current = DEPARTMENTS.index(moved["dept_code"])
        successor["dept_code"] = DEPARTMENTS[(current + 1) % len(DEPARTMENTS)]
        successor["effective_date"] = schema.format_date(date(2026, 7, 1))
        rows.append(successor)
        rows.sort(key=lambda row: (row["cc_code"], row["effective_date"]))

    return rows


def _rate(micros: int) -> Decimal:
    """Millionths to a rate. The division is exact, so the quantize fixes the width
    and never rounds. Mirrors `_cents` in entries.py."""
    return (Decimal(micros) / schema.RATE_MICROS).quantize(schema.RATE_PLACES)


def fx_rates(seed: int, months: list[date]) -> list[dict[str, object]]:
    """A rate per currency per day of the generated range."""
    rng = stream_for(seed, DIMENSIONS)
    rows = []
    for currency in CURRENCIES:
        centre = (
            schema.RATE_MICROS
            if currency == BASE_CURRENCY
            else rng.randrange(RATE_MIN_MICROS, RATE_MAX_MICROS)
        )
        for month in months:
            day = month
            while day.month == month.month:
                # Drawn for every currency, including the base, whose value is then
                # discarded. CNY is first in CURRENCIES, so dropping these draws would
                # shift EUR, USD and GBP for a reason unrelated to precision.
                drift = rng.randrange(-DRIFT_PPM, DRIFT_PPM + 1)
                micros = (
                    schema.RATE_MICROS
                    if currency == BASE_CURRENCY
                    else centre * (schema.RATE_MICROS + drift) // schema.RATE_MICROS
                )
                rows.append(
                    {
                        "currency": currency,
                        "rate_date": schema.format_date(day),
                        "rate_to_base": schema.format_rate(_rate(micros)),
                    }
                )
                day += timedelta(days=1)
    return rows
