-- One row per unit, from the unit crosswalk. source_spellings = how many ways the unit arrives in source data.
select
    unit_code,
    any_value(unit_name)  as unit_name,
    count(*)              as source_spellings
from {{ source('ref', 'unit_crosswalk') }}
where unit_code is not null
group by unit_code
