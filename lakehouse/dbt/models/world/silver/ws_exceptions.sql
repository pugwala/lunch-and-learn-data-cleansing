{{ config(alias='exceptions', materialized='view', tags=['world_monthly']) }}
-- One row per flagged value across the world's silver tables (the issues lists), like silver.hero_offenders_exceptions.
SELECT 'location' AS table_name, location_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_location') }}
UNION ALL
SELECT 'power' AS table_name, power_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_power') }}
UNION ALL
SELECT 'team' AS table_name, team_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_team') }}
UNION ALL
SELECT 'person' AS table_name, person_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_person') }}
UNION ALL
SELECT 'person_power' AS table_name, person_power_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_person_power') }}
UNION ALL
SELECT 'team_member' AS table_name, team_member_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_team_member') }}
UNION ALL
SELECT 'incident' AS table_name, incident_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_incident') }}
UNION ALL
SELECT 'participant' AS table_name, participant_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_participant') }}
UNION ALL
SELECT 'power_use' AS table_name, power_use_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_power_use') }}
UNION ALL
SELECT 'damage' AS table_name, damage_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_damage') }}
UNION ALL
SELECT 'charge' AS table_name, charge_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_charge') }}
UNION ALL
SELECT 'disposition' AS table_name, disposition_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_disposition') }}
UNION ALL
SELECT 'mission' AS table_name, mission_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_mission') }}
UNION ALL
SELECT 'mission_member' AS table_name, mission_member_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_mission_member') }}
UNION ALL
SELECT 'mission_incident' AS table_name, mission_incident_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_mission_incident') }}
UNION ALL
SELECT 'incident_report' AS table_name, incident_report_id AS id, unnest(issues, recursive := true) FROM {{ ref('ws_incident_report') }}
