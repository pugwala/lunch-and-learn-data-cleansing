-- period_readiness has exactly one row per product and month-end: a product that lists an input twice, or a
-- missing input file, would show up here as duplicates or gaps.
select product, month_end, count(*) as rows_for_the_month
from {{ ref('period_readiness') }}
group by product, month_end
having count(*) <> 1
