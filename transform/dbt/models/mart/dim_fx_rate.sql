-- A rate is a slowly changing attribute of a currency, so it is built by the same macro
-- as the two source dimensions: a rate runs from the day it was published until the day
-- before the next one, which is what puts the weekend inside Friday's interval without a
-- rule written for weekends. See docs/adr/0029 and 0030.
--
-- `rate_to_base` is cast after the fingerprint is taken, never before: the fingerprint is
-- over the text as it arrived, so it cannot depend on how numeric formatting renders six
-- decimal places. See docs/adr/0013.
--
-- The base currency is published every day at exactly 1.000000, so the fingerprint
-- collapses all of it into a single version - which is the collapse doing its job, not a
-- special case written for it.
{{ scd2('fx_rate', ['currency'], ['rate_to_base'],
        effective='rate_date', casts={'rate_to_base': 'numeric(18,6)'}) }}
