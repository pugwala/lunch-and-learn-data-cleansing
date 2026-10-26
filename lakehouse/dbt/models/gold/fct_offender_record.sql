-- One row per offender record: the keys and measures analysts and the semantic layer need.
-- Names, aliases, free-text notes and date of birth stay in silver; age_at_sentencing carries what reports need.
with exceptions as (
    select offender_number, count(*) as exception_count
    from {{ source('silver', 'hero_offenders_exceptions') }}
    group by offender_number
)

select
    s.offender_number,
    s.sid_number,
    s.unit_code,
    s.offense_code_std                         as offense_code,
    s.offense_against,
    s.violence_code_std                        as violence_code,
    s.violence_code_std_rank                   as violence_code_rank,
    s.custody_level,
    s.custody_level_rank,
    s.threat_level,
    s.threat_level_rank,
    s.stg_group,
    s.stg_status,
    s.gender,
    s.race_code,
    s.race_omb,
    s.religion_group,
    s.religion_family,
    s.sentence_date,
    date_trunc('month', s.sentence_date)::date as sentence_month,
    s.projected_release_date,
    s.intake_time,
    s.age_at_sentencing,
    s.disciplinary_points,
    s.restitution_owed,
    s.escape_risk,
    s.protective_custody,
    s.dampener_required,
    coalesce(e.exception_count, 0)             as exception_count,
    coalesce(e.exception_count, 0) > 0         as has_exception
from {{ source('silver', 'hero_offenders') }} as s
left join exceptions as e using (offender_number)
