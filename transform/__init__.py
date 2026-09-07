"""Modelling the raw layer into query-ready shapes.

`spark/` holds the work that has to scale - the SCD2 dimension load, the point-in-time
join, monthly aggregation. `dbt/` holds the relational layer. The two do not overlap;
see the READMEs in each.
"""
