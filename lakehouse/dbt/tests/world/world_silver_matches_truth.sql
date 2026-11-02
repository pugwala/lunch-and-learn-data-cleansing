{{ config(tags=['world']) }}
-- Silver equals the clean truth (the generator's Parquet under var world_root) in every cell, except where the
-- answer key says it can't: an unknown token hid the value (silver null), an orphaned link, an out-of-order date
-- or a link to a duplicate person is kept as written (and flagged), or a two-digit year was read a century off
-- because the true year is outside the pivot's 1927-2026 (1925 written 25 reads as 2025, 2071 written 71 as
-- 1971). Names compare in upper case. Rows returned are cells silver got wrong.
{%- for table, s in world_spec().items() %}
{%- if not loop.first %}
UNION ALL
{%- endif %}
SELECT '{{ table }}' AS table_name, c.id, c.u.c AS column_name, c.u.s AS silver, c.u.t AS truth, k.kind, k.as_written
FROM (
    SELECT ws.{{ table }}_id AS id, unnest([
        {%- for col in s["cols"] if col[1] != "id" %}
        {'c': '{{ col[0] }}',
         {%- if col[1] in ("name", "person_name") %}
         's': upper(ws.{{ col[0] }}::VARCHAR), 't': upper(tr.{{ col[0] }}::VARCHAR)}
         {%- else %}
         's': ws.{{ col[0] }}::VARCHAR, 't': tr.{{ col[0] }}::VARCHAR}
         {%- endif %}{{ "," if not loop.last }}
        {%- endfor %}
    ]) AS u
    FROM {{ ref('ws_' ~ table) }} ws
    JOIN read_parquet('{{ var("world_root") }}/{{ table }}/*.parquet') tr ON tr.{{ table }}_id = ws.{{ table }}_id
) c
LEFT JOIN read_parquet('{{ var("world_root") }}/answer_key/{{ table }}/*.parquet') k
    ON k.id = c.id AND k.column_name = c.u.c
WHERE c.u.s IS DISTINCT FROM c.u.t
  AND NOT coalesce(
        (k.kind = 'unknown' AND c.u.s IS NULL)
        OR (k.kind IN ('orphan', 'duplicate_ref', 'out_of_order') AND c.u.s = k.as_written)
        OR (k.kind = 'date_format' AND regexp_full_match(k.as_written, '\d{1,2}/\d{1,2}/\d{2}')
            AND year(try_cast(c.u.t AS DATE)) NOT BETWEEN 1927 AND 2026
            AND try_cast(c.u.s AS DATE) IN (try_cast(c.u.t AS DATE) + INTERVAL 100 YEAR,
                                            try_cast(c.u.t AS DATE) - INTERVAL 100 YEAR)), false)
{%- endfor %}
