{{ config(alias='fct_damage', materialized='incremental', incremental_strategy='delete+insert', unique_key='damage_id', on_schema_change='fail',
          tags=['world_monthly']) }}
-- One row per damage assessment: cost, insurance, repair, and days from the fight to the repair.
SELECT d.damage_id, d.incident_id, d.incident_month, d.asset_type, d.cost_usd, d.insured, d.insurance_paid_usd,
       d.repair_status, d.repaired_on, (d.repaired_on - i.started_at::DATE)::INTEGER AS days_to_repair,
       len(d.issues) > 0 AS has_issues
FROM {{ ref('ws_damage') }} d
LEFT JOIN {{ ref('ws_incident') }} i USING (incident_id)
WHERE {{ w_in_window('d.incident_month') }}
