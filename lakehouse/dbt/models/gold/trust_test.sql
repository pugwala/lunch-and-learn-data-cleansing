-- The Trust Test scorecard, one row per question: the answer from a default load and from the cleansed, typed
-- data. Computed by the course's own lnl.trust in the loader, so the dashboard shows exactly what notebook 4 shows.
select
    question_id,
    question,
    as_loaded,
    cleansed,
    changed
from {{ source('silver', 'trust_test') }}
