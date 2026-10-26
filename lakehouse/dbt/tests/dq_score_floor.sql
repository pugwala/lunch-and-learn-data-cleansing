{{ config(severity='warn') }}
-- Quality floor: warn (don't stop) when any rule scores under 95%. The warning count is the alert signal.
select dimension, column_name, rule, score_pct
from {{ ref('dq_scorecard') }}
where score_pct < 95
