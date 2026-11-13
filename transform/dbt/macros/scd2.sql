{# Turning an effective-dated source dimension into validity intervals.

   Three dimensions need the same thing, so it is written once. The source declares when
   each change took effect - every version in every extract, keyed on (code,
   effective_date) - so the history is read, not inferred. `valid_from` is the date the
   change took effect in the business, never the date this pipeline noticed it: inferring
   it from run-to-run differences would put every version's `valid_from` at "today" the
   first time a fresh clone ran, and every historical report would collapse to one
   interval.

   See docs/adr/0024 (the open interval ends at a sentinel), 0025 (the surrogate key is
   derived from the version it identifies) and 0029. #}

{% macro injective(columns) %}
  {#- The rendering docs/adr/0025 settles on. Every field carries its length, because
      the separator alone is not enough: a source value may hold any character, that one
      included, and without the length prefix ("A", "B<US>C") and ("A<US>B", "C") render
      identically. The same rendering ingest.raw.checksum uses. -#}
  {%- set parts = [] -%}
  {%- for column in columns -%}
    {%- do parts.append("length(" ~ column ~ ")::text || ':' || " ~ column) -%}
  {%- endfor -%}
  {{ parts | join(" || chr(31) || ") }}
{% endmacro %}


{% macro scd2(source_table, natural_key, attributes, effective='effective_date', casts={}, nullable=[]) %}

with source as (
    select
        {% for column in natural_key %}{{ column }},{% endfor %}
        {% for column in attributes %}{{ column }},{% endfor %}
        {{ effective }} as effective_date
    from {{ source('landing', source_table) }}
),

-- Collapse a version that restates the previous one without changing anything. A new
-- effective date carrying identical attributes is not a change, and counting it as one
-- would make "how many versions does this key have" a function of how many times the
-- source re-exported it. The fingerprint is taken over the text as it arrived, so it
-- does not depend on how any engine formats a decimal.
fingerprinted as (
    select
        *,
        encode(sha256(({{ injective(attributes) }})::bytea), 'hex') as _fingerprint,
        lag(encode(sha256(({{ injective(attributes) }})::bytea), 'hex')) over (
            partition by {{ natural_key | join(', ') }} order by effective_date
        ) as _previous
    from source
),

distinct_versions as (
    select * from fingerprinted
    where _previous is null or _previous <> _fingerprint
),

-- The interval is built from the next version's effective date, so abutting exactly -
-- and therefore neither overlapping nor leaving a gap - is a property of the
-- construction rather than something asserted afterwards.
chained as (
    select
        {% for column in natural_key %}{{ column }},{% endfor %}
        {#- The fingerprint above was taken over the text as it arrived; this is where
            that text becomes what the mart means by it. An empty field is where raw
            records absence (docs/adr/0015), and the mart is the layer the application
            reads, so it is here that it becomes null. -#}
        {% for column in attributes %}
          {%- if column in casts %}{{ column }}::{{ casts[column] }} as {{ column }},
          {%- elif column in nullable %}nullif({{ column }}, '') as {{ column }},
          {%- else %}{{ column }},{% endif %}
        {% endfor %}
        effective_date::date as valid_from,
        lead(effective_date::date) over (
            partition by {{ natural_key | join(', ') }} order by effective_date
        ) as _next
    from distinct_versions
)

select
    -- Derived from the version it identifies rather than issued in sequence, so it is
    -- recomputable and needs no state. `valid_from` is rendered with an explicit format
    -- rather than by implicit cast, so the key does not move if a session's date
    -- formatting does.
    encode(sha256((
        {{ injective(natural_key + ["to_char(valid_from, 'YYYY-MM-DD')"]) }}
    )::bytea), 'hex') as surrogate_key,
    {% for column in natural_key %}{{ column }},{% endfor %}
    {% for column in attributes %}{{ column }},{% endfor %}
    valid_from,
    coalesce(_next - interval '1 day', date '9999-12-31')::date as valid_to,
    _next is null as is_current
from chained

{% endmacro %}
