-- Passed through from the landing layer. The SCD2 intervals were built by
-- transform/spark/scd2.py and are not recomputed here; this model exists so the mart
-- is a closed set of tables the application can join, and so the interval gate has
-- something in the mart to check.

select * from {{ source('landing', 'dim_fx_rate') }}
