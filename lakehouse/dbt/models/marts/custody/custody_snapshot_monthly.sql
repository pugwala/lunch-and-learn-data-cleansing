-- One row per offender record per month-end it is open (sentenced on or before the month-end, projected release
-- after it): a periodic snapshot of who is in custody. It carries the SID, so the semantic layer counts distinct
-- people at any breakdown; people counts aren't stored, because they don't add up across units or levels (a person
-- with two open records would be counted twice). Complete months only. Records with no sentence or release date
-- can't be placed in time and are left out; snapshot_coverage counts them.
with bounds as (
    select
        date_trunc('month', min(sentence_date))::date             as first_month,
        (date_trunc('month', current_date) - interval 1 day)::date as last_month_end
    from {{ ref('fct_offender_record') }}
),

-- Month starts, then each one's last day: adding a month to the 31st would drift to the 28th after February.
month_ends as (
    select last_day(unnest(generate_series(first_month, last_month_end, interval 1 month))::date) as month_end
    from bounds
)

select
    me.month_end,
    f.offender_number,
    f.sid_number,
    f.unit_code,
    f.custody_level,
    f.threat_level,
    f.offense_code,
    f.offense_against,
    f.violence_code
from month_ends me
join {{ ref('fct_offender_record') }} f
  on f.sentence_date <= me.month_end
 and f.projected_release_date > me.month_end
