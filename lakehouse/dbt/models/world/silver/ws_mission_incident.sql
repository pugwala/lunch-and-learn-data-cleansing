{{ config(alias='mission_incident', materialized='incremental', incremental_strategy='delete+insert', unique_key='mission_incident_id', on_schema_change='fail',
          tags=['world_monthly']) }}
-- Typed and checked from bronze (macros/world.sql, world_silver), one month at a time when Dagster runs a
-- partition (vars world_from, world_to); problems in issues, never dropped.
{{ world_silver('mission_incident') }}
