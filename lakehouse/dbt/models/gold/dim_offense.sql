-- FBI NIBRS offense codes used by the course.
select
    code           as offense_code,
    offense        as offense_description,
    crime_against
from {{ source('ref', 'nibrs_offenses') }}
