-- The entry fact, widened so the application computes nothing. Every name, the base
-- currency amount and the three surrogate keys are columns here rather than something a
-- reader has to join for. See macros/attributed.sql for the join shape.
--
-- No column names the build that wrote this. See docs/adr/0038.
{{ entry_facts('gl_entry') }}
