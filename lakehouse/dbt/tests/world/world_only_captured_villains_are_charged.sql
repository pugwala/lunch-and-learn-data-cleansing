{{ config(tags=['world']) }}
-- Business rule "Only a captured villain is charged" (glossary.yml). Rows returned are charges, neither they nor
-- the participant flagged, against someone who wasn't a captured villain in that fight.
SELECT c.charge_id, p.participant_id, p.side, p.status
FROM {{ ref('wg_fct_charge') }} c
JOIN {{ ref('wg_fct_participation') }} p USING (participant_id)
WHERE (p.side <> 'villain' OR p.status <> 'captured')
  AND NOT c.has_issues AND NOT p.has_issues
