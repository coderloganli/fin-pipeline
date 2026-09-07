# The surrogate key is derived from the version it identifies, not issued in sequence

## Context

Each version of a dimension row needs a key the fact table can carry, distinct from
the natural key, because a natural key identifies the cost centre and the fact needs
to point at one *version* of it.

The conventional answer is a sequence: the loader issues 1, 2, 3 as versions are
created. The alternative is to derive the key from the thing it identifies - here, a
hash over the natural key and the version's `valid_from`.

## Decision

`surrogate_key` is SHA-256 over the natural key columns and `valid_from`, each
rendered as `<length>:<value>` and joined by U+001F, the ASCII unit separator - the
same rendering `ingest/raw.py`'s row checksum already uses. Hex. No counter, no state,
no coordination.

## Reasoning

`docs/product.md` states that re-running is expected and writes are idempotent, and
`build-the-mart-models` will be accepted on the fact tables' row counts and checksums
being unchanged across three consecutive runs. A sequence cannot hold that up. It
needs state that survives between runs, and under Spark the obvious way to produce one
- `monotonically_increasing_id` - is a function of the partition a row lands in, so the
same input re-partitioned yields different keys. The foreign keys in `fct_gl_entry`
would then change on a rerun that changed nothing, and the acceptance check would fail
for a reason that has nothing to do with the data.

A derived key is stable across runs, across machines, across partition counts, and
across a rebuild from scratch. It also removes a class of question that a sequence
invites: what the loader does when it is run twice concurrently, and what happens to
the counter when staging is rebuilt. Staging is a derived layer that is expected to be
rebuilt - see `docs/adr/0026` - and a rebuilt sequence would renumber a dimension that
had not changed.

Both sides of a join can compute the key independently, without a lookup. That is not
needed today and is the reason the streaming path will be able to attribute an entry
without reading the dimension back.

The rendering has to be injective, and a separator alone is not. This was already
settled once, for the raw layer's checksum, and the comment at `ingest/raw.py`'s
`UNIT_SEPARATOR` states it: a CSV field may hold any character, that one included, so
without a length in front of each field the values `("A", "B<US>C")` and
`("A<US>B", "C")` render identically and hash the same. A test asserts that no contract
forbids the character, so nothing upstream is keeping it out. The natural-key columns
here are unconstrained strings, not pattern-limited codes.

Reusing that rendering rather than inventing a second one means there is one answer in
this repository to "how is a row turned into bytes to be hashed", and it is the one
that was already argued through. The implementation differs - the checksum renders
Python dicts, the key renders Spark columns - so the rule is shared and the code is
not; the test that pins an expected digest for a known version is what keeps the two
from drifting.

`valid_from` is rendered with an explicit `yyyy-MM-dd` format rather than by implicit
cast, so the key does not change if a session's default date formatting does.

`valid_from` is in the key rather than `effective_date` even though they hold the same
value, because `valid_from` is what the staging row means and the two would come apart
if a version were ever synthesised rather than read.

The costs are real and small here. A 64-character hex string is a wider foreign key
than an integer, and joins on it are slower - against a chart of forty accounts and
twelve cost centres, which are broadcast to every executor, that is not measurable. It
is not human-readable, so it is not the thing to look at when debugging; the natural
key and `valid_from` are carried alongside it precisely so nobody has to. And a hash
can collide: at SHA-256 over inputs of this size, that is not a risk worth modelling.
