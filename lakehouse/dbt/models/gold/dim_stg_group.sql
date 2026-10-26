-- Fictional STG groups from the crosswalk. The crosswalk maps NONE, N/A, NO and similar to the group "None",
-- which means no recorded affiliation, so it is kept as a row but marked as not an affiliation.
select
    stg_group,
    count(*)              as known_aliases,
    stg_group <> 'None'   as is_affiliation
from {{ source('ref', 'stg_groups') }}
group by stg_group
