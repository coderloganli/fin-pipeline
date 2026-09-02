# A raw row carries two run identifiers, not one

## Context

The raw layer has to answer "which run put this row here". A single `run_id` column
seems to be what that asks for, until the merge is taken into account.

`docs/adr/0016-a-merge-rewrites-the-affected-partitions-whole.md` rewrites a whole
accounting period whenever the batch touches it. The rows that did not change are
rewritten along with the ones that did. So every write has to decide what the
identifier on an unchanged row becomes.

Stamping the current run onto every row written is the cheap reading, and it destroys
the question. The open accounting period is touched by every nightly run, so its rows
would be restamped every night; `--full` restamps the entire table in one command. The
answer to "which run brought this row" would permanently be "the most recent one".

Worse, the column would not go empty. It would hold a plausible value that points at a
run whose watermark window has nothing to do with the row, and no existing gate would
notice: `docs/adr/0017-the-raw-checksum-is-over-rows-not-bytes.md` keeps ingestion
metadata outside the checksum, so a rerun still matches.

The opposite reading — keep the run that first brought the row — answers lineage and
loses the operational question. When a partition file is malformed, the run that
wrote those bytes is the one worth knowing.

## Decision

Every raw row carries two columns, written by `ingest.raw.write_partition` and by
nothing else:

- `_first_run_id` — the run that first landed this primary key. Preserved across
  every rewrite, including partition rewrites the row did not cause, eviction from a
  partition it moved out of, and the whole-table replacement a table declaring no
  watermark is loaded with.
- `_last_run_id` — the run that last wrote this row into raw, whether or not it
  changed the row's declared columns. Rewriting a partition sets it on every row in
  that partition.

Both are outside the contract and outside the checksum. `read_partition` returns them
only when asked (`metadata=True`), so existing callers still see the declared columns
and nothing else; `write_partition` takes `run_id` as a required keyword argument, so
a write path that forgot to carry the identifiers fails rather than dropping them.

The leading underscore marks them as belonging to ingest rather than to the source. A
source table is free to grow a column called `first_run_id`; it is not free to grow
one called `_first_run_id`, because nothing in a general ledger is spelled that way.

## Reasoning

The two columns answer two different questions and each is the wrong answer to the
other. Lineage — the chain from an explanation, to the entries it cites, to the run
that landed them — needs the first. Forensics — which run wrote the file that is
wrong — needs the last. Choosing one column means choosing which question the platform
can no longer answer, in exchange for one column of storage.

Preserving `_first_run_id` is close to free where it matters most. `merge_partition`
already reads the partition back to merge by primary key, so the prior identifiers of
every key already in that partition are in hand before anything is written.

Two paths need more than that, and both pay in the same currency. A row whose
accounting date moved arrives in a partition that has never held it, while the copy
carrying its identifier sits in the partition it is about to be evicted from — so the
batch is stamped from a table-wide lookup of the key columns and `_first_run_id` before
it is grouped. That is the scan `evict_moved_keys` already makes on every run, one
column wider. The whole-table replacement path does not open the old file at all, and
pays the same lookup to keep the rule identical on every table — those three tables are
two dimensions and a rate table.

A rule with an exception is a rule everyone has to check the exception list for, and
this one would be checked in the middle of an incident.

The cost is two string columns per row. Parquet stores a column of one repeated value
cheaply, and it is not paid twice: the ingestion time and the source file hash stay in
the run record, reachable through either identifier, rather than being repeated on
every row. That is a deviation from the design note that listed ingestion time and
source hash as raw columns, and it is deliberate — see
`docs/adr/0020-the-source-file-hash-belongs-to-the-run.md`.
