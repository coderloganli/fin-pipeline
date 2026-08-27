# The generator streams rows to CSV under data/source/

summary: Entries are yielded and written a row at a time so peak memory does not track row count; the format stays CSV, written byte-identically on every platform, and the directory is `source/` rather than `raw/`.

## Context

The performance numbers this project will publish — row counts, nightly run time,
the before-and-after of Spark tuning — are all measured against data this generator
produces. Nothing else feeds them. That makes scale a requirement of the generator
now rather than something to revisit later, and it rules out the shape most
generators start with: build the rows in a list, write the list.

The tables do not share a size. Accounts, cost centres and exchange rates are
hundreds of rows. `gl_entry` is meant to reach tens of millions.

## Decision

**Entries are yielded, not collected.** `entries.py` produces rows one at a time and
`writers.py` writes each as it arrives. The full set is never held. Peak memory
therefore tracks the dimension tables, which are small, and not the fact table,
which is not. Dimensions are still built in memory, because at that size the
simplicity is worth more than the consistency.

This is asserted by measurement, not by inspection: a test generates ten thousand
rows and a hundred thousand, compares `tracemalloc` peaks, and requires that a
tenfold increase in rows produces at most a twofold increase in peak memory. The
bound is a number rather than a judgement, so the test does not turn on machine
noise.

**The format stays CSV**, matching the architecture diagram. At the target scale
that is several gigabytes on disk.

**How the bytes are written is fixed rather than left to the platform**, because two
tests compare files byte for byte and the machine this is developed on is Windows:

- UTF-8, no BOM
- files opened with `newline=""`
- `lineterminator` set explicitly to the single character LF
- column order taken from `schema.py`
- amounts rendered from `Decimal` at a fixed two decimal places, never from float
- dates as `YYYY-MM-DD`

Left to its defaults, Python's csv module ends rows with CRLF on Windows, and with
CR-CR-LF if the file was opened without `newline=""`. The same data would then
produce different bytes on different machines, and a reproducibility test that
compares bytes would be asserting something about the platform rather than about the
generator.

**Output goes to `data/source/`, not `data/raw/`.** `raw` is the layer that exists
after ingest — Parquet, partitioned by accounting period, carrying ingest metadata.
What the generator writes is the source extract that ingest reads.

## Reasoning

Three consequences of the target scale are settled here rather than discovered later.
`entry_id` is a monotonic counter, which needs no global view and stays reproducible.
A voucher's lines are generated and balanced together, then written and forgotten, so
balancing never requires holding rows that have already been emitted. And the planted
anomalies are constructed, not detected: an account that grows fifty per cent a month
for three months is built that way, rather than generated and then found by scanning.
That last one is what makes streaming possible at all — finding anomalies after the
fact would mean keeping everything.

Streaming is the whole reason this is a decision and not an implementation detail.
Collecting first would work for every test in this task and fail only at the point
the numbers actually get measured, in the last few days of the schedule, when there
is no time to rewrite it. The cost of streaming now is a slightly more awkward shape
in one module.

CSV was chosen over gzip with the size known. Gzip would cut the disk cost
substantially and change nothing else, but it makes the output something you cannot
open and read, and this repository is a demonstration as much as a pipeline — being
able to look directly at the data the tests are asserting on is worth more here than
the disk. If the size becomes a practical obstacle, this record changes; the writer
is the only module that would need to.

The `source` versus `raw` distinction looks pedantic and is not. Both words are in
use in this project already, for different layers with different formats and
different owners. A generator that wrote to `data/raw/` would read, to anyone opening
the directory, as though the raw layer were CSV and unpartitioned — and the later
tasks that build ingest are exactly where that misreading would cost something.
