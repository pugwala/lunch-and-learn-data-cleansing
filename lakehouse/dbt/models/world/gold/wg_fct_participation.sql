{{ config(alias='fct_participation', materialized='incremental', incremental_strategy='delete+insert', unique_key='participant_id', on_schema_change='fail',
          tags=['world_monthly']) }}
-- One row per person in a fight: the resolved person, side, role and what became of them.
SELECT participant_id, incident_id, incident_month, person_key, side, role, status, len(issues) > 0 AS has_issues
FROM {{ ref('ws_participant') }}
WHERE {{ w_in_window('incident_month') }}
