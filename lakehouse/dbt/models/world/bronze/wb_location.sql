{{ config(alias='location') }}
-- depends_on: {{ source('landing', 'world') }}
-- The messy CSV as written (lakehouse/universe/mess.py): every column VARCHAR, blank as null.
{{ world_bronze('location') }}
