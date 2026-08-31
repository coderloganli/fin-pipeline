# The raw checksum is computed over rows, not over file bytes

## Context

The acceptance criterion for the incremental load is that the same batch loaded three
times leaves the raw layer's row count and checksum unchanged. Something has to
define what that checksum is over.

Hashing the Parquet files is the obvious reading and the wrong one. A Parquet file
carries writer metadata — the library and version that produced it — and its bytes
depend on compression settings and on how the writer chose its row groups. Two files
holding identical rows may differ, and the criterion would then be measuring the
build environment.

There is a second pressure. `record-every-pipeline-run` will stamp ingestion metadata
onto the raw layer: run identifier, ingestion time, source file hash. Ingestion time
differs on every run by construction. A checksum that covered it would be guaranteed
to change on every rerun, and the property this ticket exists to establish would be
unverifiable the moment that ticket lands.

## Decision

`ingest.raw.checksum(contract, raw_dir)` reads every partition of a table, takes only the columns
the contract declares, renders each row as its field values joined by a unit
separator, sorts the rendered rows, and returns the SHA-256 of their concatenation.

Ingestion metadata, whenever it arrives, is not a declared contract column and is
therefore outside the checksum by construction rather than by a list that has to be
maintained.

## Reasoning

Sorting rather than trusting file order makes the checksum a property of the set of
rows. It is stronger than needed today — the partitions are already written sorted,
see `docs/adr/0015-raw-lands-every-column-as-text.md` — and that is deliberate: the
checksum is the instrument the acceptance criterion is read with, and an instrument
that shares an assumption with the thing it measures cannot detect that the
assumption broke.

Rendering fields as text costs nothing here because raw already holds them as text,
and it means the digest does not depend on how Parquet encoded a column.

The cost is that the whole table is read and its rendered rows held to be sorted.
This is a verification path, not the load path, and saying so is cheaper than
building a streaming digest that the acceptance test is the only caller of.
