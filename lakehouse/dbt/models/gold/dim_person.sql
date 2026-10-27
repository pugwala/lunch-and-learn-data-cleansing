-- One row per known person (SID), with facts derived from their offender records only: no names, birth dates or
-- other personal data, which stay in silver. Records with no SID belong to no known person and aren't here.
with records as (
    select offender_number, sid_number, sentence_date, projected_release_date
    from {{ ref('fct_offender_record') }}
    where sid_number is not null
),

-- People with two records open at the same time (concurrent sentences): counting records would count them twice.
concurrent as (
    select distinct a.sid_number
    from records a
    join records b
      on a.sid_number = b.sid_number
     and a.offender_number <> b.offender_number
     and a.sentence_date < b.projected_release_date
     and b.sentence_date < a.projected_release_date
)

select
    sid_number,
    count(*)                                         as record_count,
    min(sentence_date)                               as first_sentence_date,
    max(sentence_date)                               as latest_sentence_date,
    count(*) > 1                                     as is_repeat,
    sid_number in (select sid_number from concurrent) as has_concurrent_records
from records
group by sid_number
