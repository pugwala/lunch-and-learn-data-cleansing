{{ config(alias='dim_team') }}
-- One row per team: hero leagues and villain groups (the STG groups among them).
SELECT team_id, name, side, stg_group, founded_on, base_location_id FROM {{ ref('ws_team') }}
