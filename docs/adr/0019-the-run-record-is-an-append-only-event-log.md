# The run record is an append-only event log beside the raw layer

## Context

Every run has to leave a record: what it read, how much it wrote, how long it took,
and whether it worked. Two things have to be settled — where it lives, and what is
written when.

Where first. Postgres is the only external service this repository talks to, and the
mart will eventually want to join a run identifier. But `ingest` depends on no service
today: the watermark lives in `data/raw/_state/watermarks.json`, and a load runs with
nothing started. Making the run record a database table would mean a load could not run
without Postgres up, and would pull table creation and migration into a package that
has neither.

When, second, is the sharper question. The obvious shape is to write one record after
the run finishes, holding everything it did. That record does not exist for the run
that died — which is the run somebody is looking for at eight in the morning. A record
that is only written on the paths that worked is absent exactly when it is needed.

Writing at the start and rewriting at the end fixes that and introduces a
read-modify-write over a file that grows with every run, on a path that is already
holding the raw layer's consistency.

## Decision

`data/raw/_state/runs.jsonl`, one JSON object per line, appended and never rewritten.
A run writes two lines:

    {"event": "started",  "run_id": ..., "started_at": ..., "command": "load", ...}
    {"event": "finished", "run_id": ..., "finished_at": ..., "status": "succeeded", ...}

`status` is `succeeded` or `failed`; a failed run names the table it failed on and
carries the failure's message. `ingest.runs` folds the events by run identifier, and a
run whose `started` event has no `finished` event is reported as `interrupted` —
which is the true statement about it, and is not distinguishable from "still running"
by design.

The run identifier is `YYYYMMDDTHHMMSSZ-xxxxxx`: a UTC timestamp to the second, and six
hex characters. The timestamp orders the log to the second by eye, without parsing;
the suffix is random and keeps two runs started inside the same second apart, so those
two sort arbitrarily with respect to each other. Chronological order comes from the
file, which is appended to and never reordered - `ingest.runs` reports runs in log
order and does not sort the identifiers.

Durations are measured on a monotonic clock and reported in seconds; the wall clock
supplies `started_at` and `finished_at` only. `ingest.runs` reads the log and prints
it — the recent runs by default, one run in full with `--run`.

## Reasoning

Append-only is what makes the crash case work. There is no window in which the file is
half-rewritten, a run that dies leaves its `started` line intact, and no reader has to
trust that the last writer finished. A rewrite would have to be done the way the
watermark file is written — to a temporary file and moved over — which means holding
the whole history in memory to append one line to it.

Two events rather than one is the same argument as the watermark's ordering in
`docs/adr/0016`: what is written first is what survives the failure. The started event
records the intent — this run, these tables, this window — and the finished event
records what came of it. The pair is what lets the morning question be answered:
something started at 03:15, said it was going to read these five tables, and never
said anything again.

Reporting an unfinished run as `interrupted` rather than guessing is the same
discipline as `docs/adr/0012`: the log says what it knows. A process that is genuinely
still running looks identical, and inventing a heartbeat to tell them apart would be
building a scheduler inside an ingest package that a scheduler will eventually run.

A file rather than a table also keeps the run record readable when the database is the
thing that is broken. When the mart lands and something needs to join on a run
identifier, that is a mart-layer concern with its own loading step, and this log is the
source it loads from.

The log grows by two lines per run and is never compacted. At one run a day that is
under a megabyte a decade; a rotation policy would be code with no reader.
