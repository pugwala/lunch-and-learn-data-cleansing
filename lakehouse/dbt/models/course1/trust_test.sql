-- The Trust Test scorecard, one row per question: the answer from a default load and from the cleansed, typed
-- data. Computed by course 1's own lnl.trust and handed over (lakehouse/handoff), so the dashboard shows exactly what
-- notebook 4 shows. Part of course 1's add-on (lakehouse/course1_addon.py).
select
    question_id,
    question,
    as_loaded,
    cleansed,
    changed
from {{ source('silver', 'trust_test') }}
