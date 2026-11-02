{{ config(tags=['world']) }}
-- Business rule "People in the expanded world are counted by Person Key" (glossary.yml): world_gold.dim_person has
-- exactly one row per person_key silver resolved, and every participant's person_key is one of them. Rows returned
-- are keys gold has twice, misses, or doesn't know.
SELECT person_key, 'twice in dim_person' AS problem FROM {{ ref('wg_dim_person') }} GROUP BY 1 HAVING count(*) > 1
UNION ALL
SELECT DISTINCT person_key, 'resolved in silver, missing from dim_person' FROM {{ ref('ws_person') }}
WHERE person_key NOT IN (SELECT person_key FROM {{ ref('wg_dim_person') }})
UNION ALL
SELECT DISTINCT p.person_key, 'a participant''s person_key gold doesn''t know' FROM {{ ref('wg_fct_participation') }} p
WHERE p.person_key IS NOT NULL AND p.person_key NOT IN (SELECT person_key FROM {{ ref('wg_dim_person') }})
