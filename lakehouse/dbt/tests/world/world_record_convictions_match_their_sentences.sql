{{ config(tags=['world']) }}
-- Business rule "A record's conviction is decided on its sentence date" (glossary.yml): a charge carrying an
-- offender number is that record's conviction, decided on the record's sentence date in the course's silver. Rows
-- returned are unflagged charges where the world and the course's data disagree.
SELECT c.charge_id, c.offender_number, c.outcome, c.decided_on, s.sentence_date
FROM {{ ref('wg_fct_charge') }} c
JOIN {{ source('silver', 'hero_offenders') }} s ON s.offender_number = try_cast(c.offender_number AS BIGINT)
WHERE (NOT c.is_conviction OR c.decided_on IS DISTINCT FROM s.sentence_date)
  AND NOT c.has_issues
