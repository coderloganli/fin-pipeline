"""Modelling the raw layer into query-ready shapes.

`load.py` copies the six source tables into Postgres as they arrived, and `dbt/` turns
them into the mart: the SCD2 dimensions, the point-in-time facts and the monthly
aggregate are all models there. So every transformation is one SQL graph, and the only
edge in it that is not a `ref()` is that copy.

`promote.py` is the boundary that decides whether a build becomes the mart. `periods.py`
is the accounting-period arithmetic three callers share. `lineage.py` answers what a
source change would break, and `db.py` is the one place connection settings resolve.
"""
