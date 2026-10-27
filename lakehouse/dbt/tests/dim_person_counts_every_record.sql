-- Every fact record with a SID is counted once in dim_person: a non-empty result fails the build.
select 'dim_person' as model, sum(record_count) as counted,
       (select count(*) from {{ ref('fct_offender_record') }} where sid_number is not null) as expected
from {{ ref('dim_person') }}
having sum(record_count) <> (select count(*) from {{ ref('fct_offender_record') }} where sid_number is not null)
