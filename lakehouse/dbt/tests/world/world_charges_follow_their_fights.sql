{{ config(tags=['world']) }}
-- Business rule "A charge follows its fight, and its outcome follows the charge" (glossary.yml): filed on or after
-- the fight's day, decided on or after filing. Rows returned break it without silver having flagged the charge or
-- its court outcome (flagged rows are kept as written, so they may break it).
SELECT c.charge_id, i.started_at::DATE AS fight_day, c.filed_on, c.decided_on
FROM {{ ref('wg_fct_charge') }} c
JOIN {{ ref('wg_fct_incident') }} i USING (incident_id)
WHERE (c.filed_on < i.started_at::DATE OR c.decided_on < c.filed_on)
  AND NOT c.has_issues
