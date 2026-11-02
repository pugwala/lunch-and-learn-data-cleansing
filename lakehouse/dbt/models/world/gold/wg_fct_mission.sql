{{ config(alias='fct_mission') }}
-- One row per mission: the hero team, its target, where, when, how it ended, and how many went.
SELECT m.mission_id, m.mission_type, m.team_id, m.target_team_id, m.location_id, m.started_on, m.ended_on, m.status,
       (m.ended_on - m.started_on)::INTEGER AS days, coalesce(mm.members, 0)::INTEGER AS members,
       len(m.issues) > 0 AS has_issues
FROM {{ ref('ws_mission') }} m
LEFT JOIN (SELECT mission_id, count(*) AS members FROM {{ ref('ws_mission_member') }} GROUP BY 1) mm USING (mission_id)
