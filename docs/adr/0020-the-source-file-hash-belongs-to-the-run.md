# The source file hash belongs to the run, not to the row

## Context

The design note describing the raw layer lists its ingestion metadata as "run
identifier, ingestion time, source file hash". Taken literally, that is three columns
on every row.

Two of the three are not row-level facts. Every row a run landed from `gl_entry.csv`
came from the same file with the same digest, and every row a run wrote was written
during that run's single wall-clock span. Putting them on the row repeats one value
several million times and invites the question of what they mean when a partition
rewrite mixes rows from different runs into one file.

## Decision

The run record holds, per table it touched: the source file's SHA-256, the row counts,
the watermark range, and the run's start, end and duration. The raw row holds the two
run identifiers of
`docs/adr/0018-raw-rows-carry-two-run-identifiers.md` and nothing else.

Ingestion time is reached by looking a run identifier up in the log rather than by
being copied onto the row.

Hashing is a separate sequential pass over each source file, taken before its rows are
selected.

## Reasoning

One identifier on the row and the facts in the run record is normalisation, and the
join is a lookup in a file with two lines per run. Nothing is lost: from any row,
`_first_run_id` reaches the run that landed it and therefore the digest of the file it
came from, and `_last_run_id` reaches the run that last wrote it.

It also keeps the row honest. A digest stamped on a row would be the digest of the
file the run that wrote that row read — which, after a partition rewrite, is a file
that may have had nothing to do with that row's contents. The run record cannot make
that mistake, because there the digest is scoped to the run that actually read it.

The extra pass is the cost worth stating. The load already streams each source file
once, so hashing adds a second sequential read per table per run. It is sequential I/O
against a file the operating system has just read, and it buys the only answer to "was
the extract we ingested the same bytes as the extract they sent". If a much larger
source ever makes that pass matter, hashing while the rows stream is the change to
make, and it is a change inside one function.
