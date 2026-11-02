{{ config(alias='person') }}
-- Typed and checked from bronze, then duplicate people resolved: rows with the same SID, or (without one) the
-- same last name, first name, alias (civilians have none) and birthday, are one person, whose person_key is the
-- lowest person_id among them. The birthday is month, day and two-digit year, so a two-digit year read into the
-- wrong century still matches. The other rows stay, flagged duplicate_person on person_id.
WITH typed AS (
    {{ world_silver('person') }}
),
matched AS (
    SELECT *, CASE WHEN sid_number IS NOT NULL THEN 'sid:' || sid_number
                   WHEN last_name IS NOT NULL AND first_name IS NOT NULL AND birth_date IS NOT NULL
                        THEN 'name:' || last_name || '|' || first_name || '|' || coalesce(alias, '') || '|'
                             || strftime(birth_date, '%m%d') || (year(birth_date) % 100) END AS _match
    FROM typed
),
keyed AS (
    SELECT * EXCLUDE (_match),
           CASE WHEN _match IS NULL THEN person_id ELSE min(person_id) OVER (PARTITION BY _match) END AS person_key
    FROM matched
)
SELECT * EXCLUDE (issues, _source_file),
       CASE WHEN person_key <> person_id
            THEN list_append(issues, {'column_name': 'person_id', 'rule': 'duplicate_person'}) ELSE issues END AS issues,
       _source_file
FROM keyed
