{{ config(alias='power_use') }}
-- depends_on: {{ source('landing', 'world') }}
-- The messy CSV as written (lakehouse/universe/mess.py): every column VARCHAR, blank as null.
{{ world_bronze('power_use') }}
