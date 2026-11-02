{{ config(alias='fct_charge', materialized='incremental', incremental_strategy='delete+insert', unique_key='charge_id', on_schema_change='fail',
          tags=['world_monthly']) }}
-- One row per charge with its court outcome: who (resolved person), the NIBRS code, filed and decided dates,
-- and offender_number, the link to today's records, on a record's conviction.
SELECT c.charge_id, c.incident_id, c.incident_month, c.participant_id, p.person_key, c.offense_code, c.filed_on,
       d.outcome, d.decided_on, d.outcome IN ('convicted', 'pleaded guilty') AS is_conviction, d.offender_number,
       len(c.issues) + coalesce(len(d.issues), 0) > 0 AS has_issues
FROM {{ ref('ws_charge') }} c
LEFT JOIN {{ ref('ws_disposition') }} d ON d.charge_id = c.charge_id
LEFT JOIN {{ ref('ws_participant') }} p ON p.participant_id = c.participant_id
WHERE {{ w_in_window('c.incident_month') }}
