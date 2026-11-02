{{ config(tags=['world']) }}
-- Business rule "A power is used only while its holder holds it" (glossary.yml): the user held the power on the
-- fight's day (acquired on or before it, or on an unknown day, and not yet lost). A use with nothing flagged on it,
-- its participant or its fight, that no holding covers (a flagged holding counts as covering it), can't be squared
-- with the data. In the mess that happens when the holding it needs was orphaned (its person_id or power_id points
-- at nothing) or entered under a duplicate silver didn't resolve: about 0.4% of uses, every one explained by the
-- answer key. So this returns the unexplained uses only when they pass 1%: a real break of the rule (a power used
-- after it was lost) shows up as far more.
WITH uses AS (
    SELECT u.power_use_id, u.power_id, p.person_key, i.started_at::DATE AS day
    FROM {{ ref('wg_fct_power_use') }} u
    JOIN {{ ref('wg_fct_participation') }} p USING (participant_id)
    JOIN {{ ref('wg_fct_incident') }} i ON i.incident_id = u.incident_id
    WHERE NOT u.has_issues AND NOT p.has_issues AND NOT i.has_issues
),
held AS (
    SELECT person_key, power_id, acquired_on, lost_on, len(issues) > 0 AS flagged FROM {{ ref('ws_person_power') }}
),
unexplained AS (
    SELECT u.* FROM uses u
    WHERE NOT EXISTS (
        SELECT 1 FROM held h
        WHERE h.person_key = u.person_key AND h.power_id = u.power_id
          AND (h.flagged OR ((h.acquired_on IS NULL OR h.acquired_on <= u.day) AND (h.lost_on IS NULL OR h.lost_on > u.day))))
)
SELECT * FROM unexplained
WHERE (SELECT count(*) FROM unexplained) > 0.01 * (SELECT count(*) FROM uses)
