# transform/dbt

The relational layer: models, tests, and lineage.

dbt owns what benefits from being readable and verifiable in SQL, while PySpark owns what has to scale. The two do not overlap.

Tests gate promotion: primary key uniqueness, referential integrity, debit/credit reconciliation per voucher, SCD2 validity intervals that neither overlap nor leave gaps, row-count drift against a recent baseline, and agreement between base-currency and original-currency amounts within a rounding tolerance.

The generated lineage graph is exported as a build artefact. Its purpose is to answer what a column change would break.
