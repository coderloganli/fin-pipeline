# ingest

Lands source extracts into the raw layer and keeps the load replayable.

- **Contract validation.** Each source table has an explicit contract under `contracts/`. Additive changes pass with a warning; incompatible changes fail the run.

  ```
  python -m ingest.validate --source data/source
  ```

  Exit 0 when the source is clean or carries only warnings, 1 when it is incompatible,
  2 for a usage error. `--table` scopes the run to named tables and `--max-findings`
  sets how many findings each table collects before the rest are only counted.

  A failure states that the downstream impact is unknown. Naming the models that would
  break needs a lineage graph, and dbt has not landed — so the gap is reported rather
  than left silent. See `docs/adr/0012-the-validator-reports-rather-than-raises.md`.
- **Watermarked incremental load.** Each contract names the column its table advances
  on. `gl_entry` and `gl_adjustment` advance on `posted_at` — the source has no
  `updated_at`, and `posted_at` is already the column that says when a row landed. A
  table that names no watermark is loaded in full instead, which is how the two
  dimensions and `fx_rate` are loaded. The run reads from the stored watermark less an
  overlap window, so an update that arrives late is not skipped.

  ```
  python -m ingest.load --source data/source --raw data/raw
  ```

  Exit 0 on a clean load, 2 for a usage error. `--table` scopes the run to named
  tables, `--overlap-days` sets the window (default 7), and `--full` ignores the
  stored watermark and re-reads the whole source — the backfill path, which reaches
  the same raw layer because it goes through the same merge.

  This does not re-run contract validation. `python -m ingest.validate` is the gate;
  the DAG runs the two in order.
- **Idempotent merge.** Entries are keyed on `(entry_id, version)` and merged rather
  than inserted, so a rerun produces identical output. The batch is grouped by
  accounting period and only the periods it touches are rewritten. A row whose
  accounting date moves into another period is removed from the partition that still
  holds it, so the key stays unique across the whole table. The watermark moves only
  once every partition is written. See
  `docs/adr/0016-a-merge-rewrites-the-affected-partitions-whole.md`.

  An update that arrives after the overlap window has passed is missed. That is what a
  watermarked load is, and `--full` is what recovers from it.
- **Run records.** Every run writes its identifier, watermark range, row counts, and
  duration; downstream artefacts carry that run identifier. Not landed yet - the other
  three bullets describe what this package does today, this one describes what it is
  still for. The load reports its watermark range and row counts to its caller
  already; nothing writes them down.
