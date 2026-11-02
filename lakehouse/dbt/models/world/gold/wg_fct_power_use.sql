{{ config(alias='fct_power_use', materialized='incremental', incremental_strategy='delete+insert', unique_key='power_use_id', on_schema_change='fail',
          tags=['world_monthly']) }}
-- One row per power used in a fight, with the power's tier and how well it worked.
SELECT u.power_use_id, u.incident_id, u.incident_month, u.participant_id, u.power_id, p.tier, u.effectiveness,
       len(u.issues) > 0 AS has_issues
FROM {{ ref('ws_power_use') }} u
LEFT JOIN {{ ref('ws_power') }} p USING (power_id)
WHERE {{ w_in_window('u.incident_month') }}
