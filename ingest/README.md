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
- **Watermarked incremental load.** The watermark reads the source `updated_at` with a deliberate overlap window so late updates are not skipped.
- **Idempotent merge.** Entries are keyed on `(entry_id, version)` and merged rather than inserted, so a rerun produces identical output.
- **Run records.** Every run writes its identifier, watermark range, row counts, and duration; downstream artefacts carry that run identifier.
