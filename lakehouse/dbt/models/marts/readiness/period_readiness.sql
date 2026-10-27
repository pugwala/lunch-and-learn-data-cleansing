-- Is a data product's number for a month-end final yet? One row per product per month-end of the custody snapshot.
-- A month is final when every input the product depends on (depends_on: in products/<product>.yml) has been
-- reported complete through that month-end (readiness/closes.py report); until then it is provisional, and
-- waiting_on names the units still open. A view, not a table: a unit's close shows on the very next query.
with month_ends as (
    select distinct month_end from {{ ref('custody_snapshot_monthly') }}
),

-- The latest report per input counts, so reporting an earlier month-end reopens a month.
watermarks as (
    select input, arg_max(complete_through, reported_at) as complete_through
    from {{ source('ops', 'input_closes') }}
    group by input
),

grid as (
    select d.product, m.month_end, d.input, i.unit,
           coalesce(w.complete_through >= m.month_end, false) as is_closed
    from {{ source('ops', 'product_inputs') }} as d
    cross join month_ends as m
    join {{ source('ops', 'business_inputs') }} as i on i.input = d.input
    left join watermarks as w on w.input = d.input
)

select
    product,
    month_end,
    count(*) as inputs_required,
    count(*) filter (where is_closed) as inputs_closed,
    bool_and(is_closed) as is_final,
    case when bool_and(is_closed) then 'final' else 'provisional' end as period_status,
    string_agg(unit, ', ' order by unit) filter (where not is_closed) as waiting_on
from grid
group by product, month_end
