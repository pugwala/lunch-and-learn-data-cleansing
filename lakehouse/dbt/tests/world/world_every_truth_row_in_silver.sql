{{ config(tags=['world']) }}
-- Nothing is dropped: every row of every clean table is in silver under its own id.
{%- for table in world_spec() %}
{%- if not loop.first %}
UNION ALL
{%- endif %}
SELECT '{{ table }}' AS table_name, tr.{{ table }}_id AS id
FROM read_parquet('{{ var("world_root") }}/{{ table }}/*.parquet') tr
ANTI JOIN {{ ref('ws_' ~ table) }} ws ON ws.{{ table }}_id = tr.{{ table }}_id
{%- endfor %}
