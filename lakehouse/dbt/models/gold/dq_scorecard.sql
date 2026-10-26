-- Data-quality scorecard for the latest load: one row per dimension x column x rule.
-- Dimensions follow the course: DAMA's six, with validity split into conformity and validity.
-- Accuracy is not scored: it needs ground truth this dataset doesn't have.
{%- set metadata_cols = ['_run_id', '_ingested_at', '_source_file', '_seed', '_data_version'] %}
{%- set conformity_cols = ['race', 'religion', 'stg_affiliation', 'offense_code', 'violence_code', 'unit_location',
                           'disciplinary_points', 'restitution_owed', 'intake_time',
                           'escape_risk', 'protective_custody', 'dampener_required'] %}
{%- set date_cols = ['date_of_birth', 'sentence_date', 'projected_release_date'] %}

with bronze as (
    select * from {{ source('bronze', 'hero_offenders_raw') }}
),

silver as (
    select * from {{ source('silver', 'hero_offenders') }}
),

exc as (
    select * from {{ source('silver', 'hero_offenders_exceptions') }}
),

run as (
    select any_value(_run_id) as run_id from bronze
),

bronze_values as (
    select * exclude ({{ metadata_cols | join(', ') }}) from bronze
),

-- A value is missing when it's null, only whitespace (tabs included), or a documented "unknown" code.
cells as (
    select c.column_name, c.raw_value,
           c.raw_value is not null
             and not regexp_full_match(c.raw_value, '\s*')
             and t.token is null                                   as is_present
    from (
        select * from bronze_values
        unpivot include nulls (raw_value for column_name in (columns(*)))
    ) as c
    left join {{ source('ref', 'null_tokens') }} as t
      on t.column_name = c.column_name and upper(trim(c.raw_value)) = upper(t.token)
),

completeness as (
    select 'completeness' as dimension, column_name, 'not blank or an unknown code in source' as rule,
           count(*) as checked,
           count(*) filter (where is_present) as passed
    from cells
    group by column_name
),

present as (
    select column_name, count(*) filter (where is_present) as present
    from cells
    group by column_name
),

conformity_rules as (
    select 'conformity' as dimension, p.column_name,
           'maps to reference data or converts to its type' as rule,
           p.present as checked,
           p.present - count(e.offender_number) as passed
    from present p
    left join exc e
      on e.column_name = p.column_name and e.rule in ('domain_unmapped', 'conversion_failed')
    where p.column_name in ('{{ conformity_cols | join("', '") }}')
    group by p.column_name, p.present
),

conformity_dates as (
    {%- for col in date_cols %}
    select 'conformity' as dimension, '{{ col }}' as column_name, 'parses as a date' as rule,
           count(*) filter (where not regexp_full_match(coalesce(b.{{ col }}, ''), '\s*')) as checked,
           count(*) filter (where not regexp_full_match(coalesce(b.{{ col }}, ''), '\s*')
                              and s.{{ col }} is not null) as passed
    from bronze b
    join silver s on s.offender_number = try_cast(b.offender_number as bigint)
    {% if not loop.last %}union all{% endif %}
    {%- endfor %}
),

validity as (
    select 'validity' as dimension, 'date_of_birth' as column_name,
           'plausible for the sentence date (age 17-100, not future, after 1900)' as rule,
           (select count(*) from silver where date_of_birth is not null) as checked,
           (select count(*) from silver where date_of_birth is not null)
             - (select count(*) from exc where rule = 'dob_check') as passed
),

consistency as (
    select 'consistency' as dimension, 'violence_code_std' as column_name,
           'violence code agrees with offense against' as rule,
           (select count(*) from silver where violence_code_std is not null and offense_against is not null) as checked,
           (select count(*) from silver where violence_code_std is not null and offense_against is not null)
             - (select count(*) from exc where rule = 'offense_violence_conflict') as passed
    union all
    select 'consistency', 'race_code', 'one race code per person (SID) across records',
           count(*), count(*) filter (where codes = 1)
    from (
        select sid_number, count(distinct race_code) as codes
        from silver
        where sid_number is not null and race_code is not null and race_code not in ('U', 'D')
        group by sid_number
    )
),

uniqueness as (
    select 'uniqueness' as dimension, 'offender_number' as column_name, 'record key never repeats' as rule,
           count(*) as checked, count(distinct offender_number) as passed
    from silver
),

timeliness as (
    select 'timeliness' as dimension, '(table)' as column_name, 'loaded within the last 26 hours' as rule,
           1 as checked,
           case when max(finished_at) >= now() - interval 26 hour then 1 else 0 end as passed
    from {{ source('ops', 'load_runs') }}
),

scores as (
    select * from completeness
    union all select * from conformity_rules
    union all select * from conformity_dates
    union all select * from validity
    union all select * from consistency
    union all select * from uniqueness
    union all select * from timeliness
)

select
    (select run_id from run)                                   as run_id,
    now()                                                      as built_at,
    dimension,
    column_name,
    rule,
    checked,
    passed,
    checked - passed                                           as failed,
    case when checked > 0 then round(100.0 * passed / checked, 2) end as score_pct
from scores
