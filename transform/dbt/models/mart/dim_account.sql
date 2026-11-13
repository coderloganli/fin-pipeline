-- The chart of accounts as validity intervals. See macros/scd2.sql for the shape and
-- docs/adr/0024, 0025 and 0027 for what it forecloses.
--
-- `parent_code` is empty for a first-level account, and empty is where raw records
-- absence. The mart is the layer the application reads, so it is here that the empty
-- string becomes the null that ingest/contracts already says it means - after the
-- fingerprint, never before, or the collapse would depend on how absence is spelled.
{{ scd2('dim_account_src', ['account_code'], ['name', 'parent_code', 'account_type'],
        nullable=['parent_code']) }}
