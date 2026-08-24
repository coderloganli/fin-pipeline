# ingest

Lands source extracts into the raw layer and keeps the load replayable.

- **Contract validation.** Each source table has an explicit contract under `contracts/`. Additive changes pass with a warning; incompatible changes fail the run.
- **Watermarked incremental load.** The watermark reads the source `updated_at` with a deliberate overlap window so late updates are not skipped.
- **Idempotent merge.** Entries are keyed on `(entry_id, version)` and merged rather than inserted, so a rerun produces identical output.
- **Run records.** Every run writes its identifier, watermark range, row counts, and duration; downstream artefacts carry that run identifier.
