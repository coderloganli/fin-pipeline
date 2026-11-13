-- Passed through from the source. A supplier is not effective-dated, so it has no
-- validity intervals and no surrogate key: it is keyed on the vendor code itself. This
-- model exists so the mart is a closed set of tables the application can join.
select
    vendor_code,
    name,
    category
from {{ source('landing', 'dim_vendor') }}
