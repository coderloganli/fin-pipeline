# dags

Airflow orchestration.

- **daily** ingest, transform, test, aggregate
- **backfill** recompute only the accounting periods affected by late or restated entries
- **evaluate** run the golden set and record the score

Backfill is a first-class scenario here rather than an afterthought, which is why Airflow was chosen over a simpler scheduler.
