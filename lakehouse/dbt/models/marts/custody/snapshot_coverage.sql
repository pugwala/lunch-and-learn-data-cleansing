-- One row saying what custody_snapshot_monthly covers and what it leaves out, so a consumer can see the gap
-- instead of trusting a silent one: records with no sentence or release date can't be placed in time.
select
    count(*)                                                                                  as records_total,
    count(*) filter (where sentence_date is not null and projected_release_date is not null) as records_placed,
    count(*) filter (where sentence_date is null or projected_release_date is null)          as records_without_dates,
    count(*) filter (where sid_number is null)                                                as records_without_sid,
    (select min(month_end) from {{ ref('custody_snapshot_monthly') }})                       as first_month_end,
    (select max(month_end) from {{ ref('custody_snapshot_monthly') }})                       as last_month_end
from {{ ref('fct_offender_record') }}
