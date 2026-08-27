"""Named random streams derived from one seed.

Each concern draws from its own stream — the dimensions, the ordinary entries, and
each failure-mode switch. Switching one on therefore leaves the data belonging to the
others exactly where it was, which is what lets a switch be tested in isolation and
what stops the outcome depending on which other switches happen to be on.

See docs/adr/0005-deterministic-generation.md.
"""

import hashlib
import random

# Stream names. They are strings rather than positions on purpose: adding one must not
# renumber the others and silently move data that existing tests assert on.
DIMENSIONS = "dimensions"
ENTRIES = "entries"
ADJUSTMENTS = "adjustments"
LATE_ENTRIES = "late_entries"
RESTATEMENTS = "restatements"
COST_CENTRE_MOVE = "cost_centre_move"
UNBALANCED_VOUCHERS = "unbalanced_vouchers"
GROWING_ACCOUNT = "growing_account"
AMOUNT_OUTLIERS = "amount_outliers"


def stream_for(seed: int, name: str) -> random.Random:
    """A generator-independent stream for `name`.

    The derivation is SHA-256 rather than Python's `hash()`. `hash()` of a string is
    salted per process, so a generator built on it produces different data in
    different runs while looking perfectly correct within any one of them — and no
    same-process test can see the difference.
    """
    digest = hashlib.sha256(f"{seed}:{name}".encode("utf-8")).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))
