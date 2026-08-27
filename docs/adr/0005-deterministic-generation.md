# One seed, several independent random streams

summary: Generation is reproducible from a seed, and each failure-mode switch draws from its own stream so that turning one on does not disturb the data belonging to another.

## Context

The generator is what every later test stands on. A scenario test plants a specific
failure — an entry backdated two months, a cost centre that moves mid-year — and
asserts the pipeline responds correctly. That only works if the planted data is the
same every time the test runs, so generation has to be reproducible from a seed.

Reproducibility alone is not enough. The switches have to be usable in combination
and testable in isolation, and the obvious implementation quietly prevents both. A
single shared random sequence means every draw is positioned by every draw before
it: switching on unbalanced vouchers consumes numbers that would otherwise have gone
to the dimensions, so the accounts, the cost centres and every later entry all shift.
Nothing is wrong with the data, but a test can no longer say "with this switch on,
this changed and nothing else did", and the outcome starts depending on the order the
switches happen to be applied.

## Decision

Generation takes one seed. Given the same seed and the same configuration, two runs
produce byte-identical files.

That seed is expanded into **named streams, one per concern** — the dimensions, the
ordinary entries, and each failure-mode switch — rather than one sequence shared by
everything. A stream is derived from the seed and its own name, so adding a stream
later does not move the existing ones.

Derivation is `SHA-256(f"{seed}:{name}")`, not Python's built-in `hash()`. `hash()`
of a string is salted per process, so a generator built on it would produce different
data in different runs while looking correct in any single one — defeating the whole
point while passing a same-process reproducibility test.

Two tests hold this up. One switches on only `unbalanced_vouchers` and requires that
the dimension files *and the entry rows outside the unbalanced vouchers* are
byte-for-byte what they are with every switch off. The other tests the derivation
directly, including from a subprocess run under a different `PYTHONHASHSEED`.

## Reasoning

The shared-sequence version is less code and is what most generators do, so it is
worth being explicit about what it costs here. It couples every switch to every
other one through the position of the cursor. The failure that produces is not a
crash — it is a test that passes or fails depending on which other switches are on,
which is the kind of defect that gets diagnosed as flakiness and worked around
rather than understood.

Deriving streams by name rather than by index is the part that keeps this durable.
Numbering them would mean that inserting a new switch renumbers the ones after it,
silently changing data that older tests assert on. The names are stable, so the
streams are.

The property is recorded as a test rather than only as this document, because a
convention nothing checks is a convention that erodes. The first version of that test
compared only the dimension files, and it was not enough: dimensions are generated
before entries, so a single shared sequence leaves them identical too. The test would
have passed against precisely the design this record rejects. Comparing the untouched
entry rows as well is what makes it discriminating, and testing the derivation
separately is what catches the `hash()` mistake, which no same-process test can see.
