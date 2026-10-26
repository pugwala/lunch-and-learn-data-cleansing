-- Gold must hold every silver record: records are flagged, never dropped. Returns a row (= fails) on mismatch.
select (select count(*) from {{ source('silver', 'hero_offenders') }}) as silver_rows,
       (select count(*) from {{ ref('fct_offender_record') }})         as gold_rows
where silver_rows <> gold_rows
