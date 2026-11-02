{{ config(alias='team') }}
-- depends_on: {{ source('landing', 'world') }}
-- The messy CSV as written (lakehouse/universe/mess.py): every column VARCHAR, blank as null.
{{ world_bronze('team') }}
