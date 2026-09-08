# transform/dbt

The mart: a star in Postgres, and the six gates over it.

dbt owns what benefits from being readable and verifiable in SQL, while PySpark owns
what has to scale. The two do not overlap, and nothing here recomputes anything
`transform/spark/` already computed — the figures arrive through `transform/load.py`
and this layer shapes them so the application joins nothing and calculates nothing.

`fct_gl_entry` and `agg_monthly_balance`, widened with the names an analyst reads,
alongside `dim_account`, `dim_cost_center`, `dim_fx_rate` and `dim_vendor`. Plus
`model_row_count`, which is not a figure anyone reports: it is the memory the drift gate
compares against.

Six gates, and each is defined by what it stops rather than by existing: primary key
uniqueness, referential integrity, debit and credit balancing per voucher, SCD2
validity intervals that neither overlap nor gap, row-count drift against a recent
baseline, and agreement between base-currency and original amounts within a rounding
tolerance. Every one has a constructed scenario in `tests/test_mart_gates.py` that turns
it red. `store_failures` is on project-wide, so a failing gate leaves its offending rows
behind rather than only a count.

`profiles.yml` lives here rather than in `~/.dbt`, so the same command works on a
machine, in CI and in an image with nothing to place first. Both schema names come from
the environment.

The lineage graph is exported as a build artefact by `transform/lineage.py`, not by
`dbt docs generate --static` — see docs/adr/0037 for why. Its purpose is to answer what
a column change would break, and `ingest/validate.py` asks it that question directly.
