{{ config(alias='code_spellings') }}
-- Every spelling silver reads as a code, lower-cased: the code itself (so any case), its words joined by a space or
-- an underscore, its abbreviation (initials of several words, or the first four letters of a long word), and the
-- known alternatives (seeds/world_code_spellings.csv). A spelling two codes could share is dropped, so each
-- spelling means one code.
WITH codes AS (
    SELECT table_name, column_name, code, string_split(replace(code, '_', ' '), ' ') AS words
    FROM {{ ref('world_codes') }}
),
candidates AS (
    SELECT table_name, column_name, code, lower(code) AS spelling FROM codes
    UNION SELECT table_name, column_name, code, lower(array_to_string(words, ' ')) FROM codes
    UNION SELECT table_name, column_name, code, lower(array_to_string(words, '_')) FROM codes
    UNION SELECT table_name, column_name, code,
                 lower(CASE WHEN len(words) > 1 THEN array_to_string(list_transform(words, w -> left(w, 1)), '')
                            WHEN length(code) > 4 THEN left(code, 4) END)
          FROM codes WHERE len(words) > 1 OR length(code) > 4
    UNION SELECT table_name, column_name, code, lower(spelling) FROM {{ ref('world_code_spellings') }}
)
SELECT table_name, column_name, spelling, any_value(code) AS code
FROM candidates
GROUP BY ALL
HAVING count(DISTINCT code) = 1
