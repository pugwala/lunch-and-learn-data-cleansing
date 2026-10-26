-- Every score is a percentage, and passed never exceeds checked.
select * from {{ ref('dq_scorecard') }}
where score_pct < 0 or score_pct > 100 or passed > checked or passed < 0
