{{ config(tags=['world']) }}
-- Silver's flags are the answer key's defects: a token where a value should be (missing_token) is exactly a key
-- unknown or null_token; an orphan exactly a key orphan; every out_of_order flag a key out_of_order; every
-- two_digit_year flag a key date_format; nothing unparseable. Rows returned are a flag the key doesn't have, or a
-- key defect silver missed (where silver can always see it).
WITH flags AS (
    SELECT table_name, id, column_name, rule FROM {{ ref('ws_exceptions') }}
    WHERE rule IN ('missing_token', 'orphan', 'out_of_order', 'two_digit_year', 'unparseable')
),
keyed AS (
    SELECT regexp_extract(filename, 'answer_key/([a-z_]+)/', 1) AS table_name, id, column_name,
           CASE kind WHEN 'unknown' THEN 'missing_token' WHEN 'null_token' THEN 'missing_token'
                     WHEN 'date_format' THEN 'two_digit_year' ELSE kind END AS rule, kind, as_written
    FROM read_parquet('{{ var("world_root") }}/answer_key/*/*.parquet', filename = true)
    WHERE kind IN ('unknown', 'null_token', 'orphan', 'out_of_order', 'date_format')
)
SELECT f.*, 'flag not in the key' AS problem FROM flags f
LEFT JOIN keyed k USING (table_name, id, column_name, rule) WHERE k.id IS NULL
UNION ALL
SELECT k.table_name, k.id, k.column_name, k.rule, 'key defect not flagged' FROM keyed k
LEFT JOIN flags f USING (table_name, id, column_name, rule)
WHERE f.id IS NULL AND k.rule IN ('missing_token', 'orphan')
