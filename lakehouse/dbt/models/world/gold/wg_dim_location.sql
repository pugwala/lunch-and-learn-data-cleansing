{{ config(alias='dim_location') }}
-- One row per place: city, district or site (custody units are sites with their unit_code).
SELECT location_id, kind, name, parent_id, city_id, site_type, unit_code, latitude, longitude
FROM {{ ref('ws_location') }}
