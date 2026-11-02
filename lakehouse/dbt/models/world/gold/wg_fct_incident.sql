{{ config(alias='fct_incident') }}
-- One row per fight: when (and its month), where (and the city), what kind, how it ended, bystanders.
SELECT i.incident_id, i.incident_month, i.started_at, i.duration_min, i.location_id, l.city_id, i.incident_type,
       i.outcome, i.bystanders_injured, i.bystanders_killed, len(i.issues) > 0 AS has_issues
FROM {{ ref('ws_incident') }} i
LEFT JOIN {{ ref('ws_location') }} l USING (location_id)
