{{ config(alias='dim_person') }}
-- One row per person, duplicates resolved (person_key): role, today's or new, gender and home place, with how
-- many times they were entered. No names or birth dates, which stay in silver (as in gold.dim_person).
SELECT person_key, any_value(sid_number ORDER BY person_id) AS sid_number, any_value(role ORDER BY person_id) AS role,
       any_value(is_core ORDER BY person_id) AS is_core, any_value(gender ORDER BY person_id) AS gender,
       any_value(home_location_id ORDER BY person_id) AS home_location_id, count(*)::INTEGER AS entries
FROM {{ ref('ws_person') }}
GROUP BY person_key
