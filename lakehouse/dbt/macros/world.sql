{#-
  The expanded world's lake layers (dataset expansion, chunk 7): bronze loads the messy CSVs as written
  (lakehouse/universe/mess.py), silver cleans and types them and flags every problem it finds (flag, don't fix),
  gold is the star the products read. world_spec() is the one description of every column that silver and the
  answer-key tests share.

  Column kinds: id (a table's own key, never messy), fk:<table> (a link), int, float, float32, money, bool, date,
  timestamp, code (spellings from the code list, seeds/world_codes.csv), offense (a NIBRS code), name (kept as
  written), person_name (upper case, as the course writes names), text (kept as written).
-#}

{% macro world_spec() %}
  {{ return({
    "location": {"cols": [["location_id", "id"], ["kind", "code"], ["name", "name"], ["parent_id", "fk:location"],
                          ["city_id", "fk:location"], ["unit_code", "text"], ["site_type", "code"],
                          ["latitude", "float"], ["longitude", "float"]]},
    "power": {"cols": [["power_id", "id"], ["name", "name"], ["tier", "code"], ["permanence", "code"],
                       ["category", "code"]]},
    "team": {"cols": [["team_id", "id"], ["name", "name"], ["side", "code"], ["stg_group", "code"],
                      ["founded_on", "date"], ["base_location_id", "fk:location"]]},
    "person": {"cols": [["person_id", "id"], ["sid_number", "int"], ["role", "code"], ["is_core", "bool"],
                        ["last_name", "person_name"], ["first_name", "person_name"], ["alias", "person_name"],
                        ["gender", "code"], ["birth_date", "date"], ["home_location_id", "fk:location"]]},
    "person_power": {"cols": [["person_power_id", "id"], ["person_id", "fk:person"], ["power_id", "fk:power"],
                              ["acquired_on", "date"], ["lost_on", "date"], ["source", "code"]],
                     "order": {"lost_on": [none, none, "acquired_on"]}},
    "team_member": {"cols": [["team_member_id", "id"], ["team_id", "fk:team"], ["person_id", "fk:person"],
                             ["status", "code"], ["joined_on", "date"], ["left_on", "date"]],
                    "order": {"left_on": [none, none, "joined_on"]}},
    "incident": {"cols": [["incident_id", "id"], ["started_at", "timestamp"], ["duration_min", "int"],
                          ["location_id", "fk:location"], ["incident_type", "code"], ["outcome", "code"],
                          ["bystanders_injured", "int"], ["bystanders_killed", "int"]]},
    "participant": {"cols": [["participant_id", "id"], ["incident_id", "fk:incident"], ["person_id", "fk:person"],
                             ["side", "code"], ["role", "code"], ["status", "code"]],
                    "month": ["incident_id", "incident"]},
    "power_use": {"cols": [["power_use_id", "id"], ["incident_id", "fk:incident"],
                           ["participant_id", "fk:participant"], ["power_id", "fk:power"],
                           ["effectiveness", "float32"]],
                  "month": ["incident_id", "incident"]},
    "damage": {"cols": [["damage_id", "id"], ["incident_id", "fk:incident"], ["asset_type", "code"],
                        ["cost_usd", "money"], ["insured", "bool"], ["insurance_paid_usd", "money"],
                        ["repair_status", "code"], ["repaired_on", "date"]],
               "order": {"repaired_on": ["incident_id", "incident", "started_at"]},
               "month": ["incident_id", "incident"]},
    "charge": {"cols": [["charge_id", "id"], ["incident_id", "fk:incident"], ["participant_id", "fk:participant"],
                        ["offense_code", "offense"], ["filed_on", "date"]],
               "order": {"filed_on": ["incident_id", "incident", "started_at"]},
               "month": ["incident_id", "incident"]},
    "disposition": {"cols": [["disposition_id", "id"], ["charge_id", "fk:charge"], ["outcome", "code"],
                             ["decided_on", "date"], ["offender_number", "text"]],
                    "order": {"decided_on": ["charge_id", "charge", "filed_on"]},
                    "month": ["charge_id", "charge"]},
    "mission": {"cols": [["mission_id", "id"], ["codename", "name"], ["mission_type", "code"],
                         ["team_id", "fk:team"], ["target_team_id", "fk:team"], ["location_id", "fk:location"],
                         ["started_on", "date"], ["ended_on", "date"], ["status", "code"]],
                "order": {"ended_on": [none, none, "started_on"]}},
    "mission_member": {"cols": [["mission_member_id", "id"], ["mission_id", "fk:mission"],
                                ["person_id", "fk:person"], ["role", "code"]]},
    "mission_incident": {"cols": [["mission_incident_id", "id"], ["mission_id", "fk:mission"],
                                  ["incident_id", "fk:incident"]],
                         "month": ["incident_id", "incident"]},
    "incident_report": {"cols": [["incident_report_id", "id"], ["incident_id", "fk:incident"],
                                 ["author_person_id", "fk:person"], ["report_kind", "code"],
                                 ["filed_at", "timestamp"], ["narrative", "text"]],
                        "month": ["incident_id", "incident"]},
  }) }}
{% endmacro %}

{#- Tokens that stand for "no value" (mess.UNKNOWN_TOKENS and mess.NULL_TOKENS); blanks and tabs count too. -#}
{% macro w_is_token(x) -%}
  (({{ x }} <> '' AND trim({{ x }}, ' ' || chr(9)) = '')
   OR upper(trim({{ x }}, ' ' || chr(9))) IN ('UNK', 'UNKNOWN', 'N/A', '?', '00/00/0000', 'NULL', 'NONE'))
{%- endmacro %}

{#- A cell as text: trimmed, with tokens and blanks as null. -#}
{% macro w_text(x) -%}
  CASE WHEN {{ w_is_token(x) }} THEN NULL ELSE nullif(trim({{ x }}, ' ' || chr(9)), '') END
{%- endmacro %}

{#- A date in any of the course's formats. A two-digit year is read with a pivot (00-26 is 2000-2026, 27-99 is
    1927-1999), so a 1925 birth date written 3/22/25 comes out 2025: silver flags those two_digit_year. -#}
{% macro w_date(t) -%}
  CASE WHEN regexp_full_match({{ t }}, '\d{1,2}/\d{1,2}/\d{2}')
       THEN make_date(CASE WHEN split_part({{ t }}, '/', 3)::INT <= 26 THEN 2000 ELSE 1900 END
                        + split_part({{ t }}, '/', 3)::INT,
                      split_part({{ t }}, '/', 1)::INT, split_part({{ t }}, '/', 2)::INT)
       ELSE try_strptime({{ t }}, ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%Y-%m-%d %H:%M:%S', '%Y%m%d',
                                   '%d-%b-%Y', '%B %d, %Y', '%b %d %Y'])::DATE END
{%- endmacro %}

{% macro w_timestamp(t) -%}
  try_strptime({{ t }}, ['%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y %I:%M:%S %p', '%Y-%m-%dT%H:%M:%S',
                         '%d-%b-%Y %H:%M:%S', '%Y%m%d%H%M%S'])
{%- endmacro %}

{#- A whole number written as 12, 12.0, 012 or (up to ten) a word. -#}
{% macro w_int(t) -%}
  coalesce(list_position(['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten'],
                         lower({{ t }})) - 1,
           try_cast(try_cast({{ t }} AS DOUBLE) AS BIGINT))
{%- endmacro %}

{#- Whole dollars from $1,234.00, USD 1,234.00, 1234.0 or - (zero). -#}
{% macro w_money(t) -%}
  CASE WHEN {{ t }} = '-' THEN 0
       ELSE try_cast(try_cast(trim(replace(replace(replace({{ t }}, 'USD', ''), '$', ''), ',', '')) AS DECIMAL(18, 2))
                     AS BIGINT) END
{%- endmacro %}

{% macro w_bool(t) -%}
  CASE WHEN lower({{ t }}) IN ('true', 't', 'y', 'yes', '1') THEN true
       WHEN lower({{ t }}) IN ('false', 'f', 'n', 'no', '0') THEN false END
{%- endmacro %}

{#- A NIBRS code from 13a, 13-A, 13 A, 290.0 or 9B (the leading zero dropped), checked against ref.nibrs_offenses
    (nibrs.codes, joined in by world_silver). -#}
{% macro w_offense(t) -%}
  {%- set s = "regexp_replace(replace(replace(upper(" ~ t ~ "), '-', ''), ' ', ''), '\\.0$', '')" -%}
  {%- set c = "CASE WHEN regexp_full_match(" ~ s ~ ", '\\d[A-Z]') THEN '0' || " ~ s ~ " ELSE " ~ s ~ " END" -%}
  CASE WHEN list_contains(nibrs.codes, {{ c }}) THEN {{ c }} END
{%- endmacro %}

{#- How one column of a kind is read from its trimmed text. -#}
{% macro w_parse(kind, name) -%}
  {%- set t = "_t_" ~ name -%}
  {%- if kind == "id" or kind.startswith("fk:") -%} try_cast({{ t }} AS BIGINT)
  {%- elif kind == "int" -%} {{ w_int(t) }}
  {%- elif kind == "float" -%} try_cast({{ t }} AS DOUBLE)
  {%- elif kind == "float32" -%} try_cast({{ t }} AS FLOAT)
  {%- elif kind == "money" -%} {{ w_money(t) }}
  {%- elif kind == "bool" -%} {{ w_bool(t) }}
  {%- elif kind == "date" -%} {{ w_date(t) }}
  {%- elif kind == "timestamp" -%} {{ w_timestamp(t) }}
  {%- elif kind == "code" -%} map_extract_value(_map_{{ name }}, lower({{ t }}))
  {%- elif kind == "offense" -%} {{ w_offense(t) }}
  {%- elif kind == "person_name" -%} upper({{ t }})
  {%- else -%} {{ t }}
  {%- endif -%}
{%- endmacro %}

{#- The Dagster monthly partition being built: vars world_from and world_to (to exclusive), or everything. Rows no
    month owns (an undated fight, a link to a missing one) are rebuilt on every run, so none is ever dropped. -#}
{% macro w_in_window(month) -%}
  {%- if var("world_from", none) -%}
  ({{ month }} IS NULL OR ({{ month }} >= DATE '{{ var("world_from") }}' AND {{ month }} < DATE '{{ var("world_to") }}'))
  {%- else -%} true {%- endif -%}
{%- endmacro %}

{#- One world silver table: every column typed, links checked, dates checked against what they follow, and an
    issues list (column_name, rule) per row: missing_token, unparseable, two_digit_year, orphan, out_of_order. A
    monthly table also says which partition run last wrote each row (_partition: its first day, or "all"). -#}
{% macro world_silver(table) %}
{%- set spec = world_spec()[table] -%}
{%- set cols = spec["cols"] -%}
{%- set order = spec.get("order", {}) -%}
{%- set month = spec.get("month") -%}
WITH raw AS (
    SELECT * FROM {{ ref("wb_" ~ table) }}
),
{%- if cols | selectattr(1, "equalto", "offense") | list %}
nibrs AS (SELECT list(code) AS codes FROM {{ source("ref", "nibrs_offenses") }}),
{%- endif %}
maps AS (
    SELECT 1 AS _one
    {%- for c in cols if c[1] == "code" %},
        (SELECT map(list(spelling), list(code)) FROM {{ ref("ws_code_spellings") }}
         WHERE table_name = '{{ table }}' AND column_name = '{{ c[0] }}') AS _map_{{ c[0] }}
    {%- endfor %}
),
txt AS (
    SELECT raw._source_file{% for c in cols %}, raw.{{ c[0] }} AS _raw_{{ c[0] }}, {{ w_text("raw." ~ c[0]) }} AS _t_{{ c[0] }}{% endfor %}
    FROM raw
),
parsed AS (
    SELECT txt.*{% for c in cols %}, {{ w_parse(c[1], c[0]) }} AS {{ c[0] }}{% endfor %}
    FROM txt CROSS JOIN maps{% if cols | selectattr(1, "equalto", "offense") | list %} CROSS JOIN nibrs{% endif %}
),
linked AS (
    SELECT p.*
    {%- for c in cols if c[1].startswith("fk:") %}
        , l_{{ c[0] }}.{{ c[1][3:] }}_id IS NOT NULL AS _found_{{ c[0] }}
    {%- endfor %}
    {%- for c in cols if c[1] == "fk:person" %}
        , k_{{ c[0] }}.person_key AS {{ c[0][:-3] }}_key
    {%- endfor %}
    {%- for later, rule in order.items() if rule[0] %}
        , a_{{ later }}.{{ rule[2] }}::DATE AS _before_{{ later }}
    {%- endfor %}
    {%- if month %}
        , m.incident_month
    {%- elif table == "incident" %}
        , date_trunc('month', p.started_at)::DATE AS incident_month
    {%- endif %}
    FROM parsed p
    {%- for c in cols if c[1].startswith("fk:") %}
    LEFT JOIN (SELECT DISTINCT try_cast({{ c[1][3:] }}_id AS BIGINT) AS {{ c[1][3:] }}_id FROM {{ ref("wb_" ~ c[1][3:]) }}) l_{{ c[0] }}
        ON l_{{ c[0] }}.{{ c[1][3:] }}_id = p.{{ c[0] }}
    {%- endfor %}
    {%- for c in cols if c[1] == "fk:person" %}
    LEFT JOIN (SELECT person_id, person_key FROM {{ ref("ws_person") }}) k_{{ c[0] }} ON k_{{ c[0] }}.person_id = p.{{ c[0] }}
    {%- endfor %}
    {%- for later, rule in order.items() if rule[0] %}
    LEFT JOIN {{ ref("ws_" ~ rule[1]) }} a_{{ later }} ON a_{{ later }}.{{ rule[1] }}_id = p.{{ rule[0] }}
    {%- endfor %}
    {%- if month %}
    LEFT JOIN (SELECT {{ month[1] }}_id, incident_month FROM {{ ref("ws_" ~ month[1]) }}) m ON m.{{ month[1] }}_id = p.{{ month[0] }}
    {%- endif %}
)
SELECT
    {%- for c in cols %}
    {{ c[0] }},
    {%- endfor %}
    {%- for c in cols if c[1] == "fk:person" %}
    {{ c[0][:-3] }}_key,
    {%- endfor %}
    {%- if month or table == "incident" %}
    incident_month,
    {%- endif %}
    list_filter([
        {%- for c in cols if c[1] != "id" %}
        CASE WHEN {{ w_is_token("_raw_" ~ c[0]) }} THEN {'column_name': '{{ c[0] }}', 'rule': 'missing_token'}
             WHEN _t_{{ c[0] }} IS NOT NULL AND {{ c[0] }} IS NULL THEN {'column_name': '{{ c[0] }}', 'rule': 'unparseable'}
             {%- if c[1] == "date" %}
             WHEN regexp_full_match(_t_{{ c[0] }}, '\d{1,2}/\d{1,2}/\d{2}') THEN {'column_name': '{{ c[0] }}', 'rule': 'two_digit_year'}
             {%- endif %}
             {%- if c[1].startswith("fk:") %}
             WHEN {{ c[0] }} IS NOT NULL AND NOT _found_{{ c[0] }} THEN {'column_name': '{{ c[0] }}', 'rule': 'orphan'}
             {%- endif %}
             {%- if c[0] in order %}
             WHEN {{ c[0] }} < {{ ("_before_" ~ c[0]) if order[c[0]][0] else order[c[0]][2] }}
                  THEN {'column_name': '{{ c[0] }}', 'rule': 'out_of_order'}
             {%- endif %}
        END,
        {%- endfor %}
    ], e -> e IS NOT NULL) AS issues,
    {%- if month %}
    '{{ var("world_from", "all") }}' AS _partition,   -- the Dagster partition (first day of its range) that wrote the row
    {%- endif %}
    _source_file
FROM linked
{%- if month %}
WHERE {{ w_in_window("incident_month") }}
{%- endif %}
{% endmacro %}

{#- A world bronze table: one messy CSV folder as written, every column VARCHAR, blank as null, plus its file. -#}
{% macro world_bronze(table) %}
SELECT * EXCLUDE (filename), parse_filename(filename) AS _source_file
FROM read_csv('{{ var("world_root") }}/messy/{{ table }}/*.csv', all_varchar = true, header = true, filename = true,
              quote = '"', escape = '"', delim = ',')
{% endmacro %}
