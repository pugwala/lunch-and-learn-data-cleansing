{{ config(alias='dim_power') }}
-- One row per power in the catalog.
SELECT power_id, name, tier, permanence, category FROM {{ ref('ws_power') }}
