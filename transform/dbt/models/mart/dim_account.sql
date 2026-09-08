-- Passed through from the landing layer, with one correction. The SCD2 intervals were
-- built by transform/spark/scd2.py and are not recomputed here; this model exists so
-- the mart is a closed set of tables the application can join, and so the interval gate
-- has something in the mart to check.
--
-- `nullif(parent_code, '')` for the reason fct_gl_entry nullifies vendor_code: raw
-- lands every column as text and writes an empty field as an empty string
-- (docs/adr/0015), because raw exists to answer whether the source really said that.
-- A top-level account has no parent, and the mart is the layer where that absence is a
-- null rather than a string nothing will ever match.

select
    surrogate_key,
    account_code,
    name,
    nullif(parent_code, '') as parent_code,
    account_type,
    valid_from,
    valid_to,
    is_current
from {{ source('landing', 'dim_account') }}
