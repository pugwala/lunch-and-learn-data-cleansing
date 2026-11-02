{{ config(alias='person_power') }}
-- depends_on: {{ source('landing', 'world') }}
-- The messy CSV as written (lakehouse/universe/mess.py): every column VARCHAR, blank as null.
{{ world_bronze('person_power') }}
