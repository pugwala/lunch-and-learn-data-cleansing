-- custody_snapshot_monthly has one row per record per month-end: a non-empty result fails the build.
select month_end, offender_number, count(*) as rows
from {{ ref('custody_snapshot_monthly') }}
group by month_end, offender_number
having count(*) > 1
