{{ config(alias='incident_report', materialized='incremental', incremental_strategy='delete+insert', unique_key='incident_report_id', on_schema_change='fail',
          tags=['world_monthly']) }}
-- Typed and checked from bronze (macros/world.sql, world_silver), one month at a time when Dagster runs a
-- partition (vars world_from, world_to); problems in issues, never dropped. The narrative is kept exactly as written
-- (only a bare token becomes null), so the mention key's places (mention_key/incident_report) hold here.
{{ world_silver('incident_report') }}
