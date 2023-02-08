with totals as(
    
select sum(price*quantity) as total_revenue,
sum(line_total_discount) as line_total_discount,
order_id 
from {{ ref('fct_orders') }}
group by 3
),

order_totals as (


select *,
coalesce(total_revenue,0) - coalesce(line_total_discount,0) as gross_revenue
from totals
),

add_order_totals as (
    select a.*,
    b.gross_revenue
    from {{ ref('fct_web_events') }} a 
    inner join order_totals b
    on a.order_id = b.order_id
)

select sum(gross_revenue) as gross_revenue,
landing_utm_source
from add_order_totals
where landing_utm_source is not null
group by 2
order by 1 desc
/*
gross_revenue   landing_utm_source
1525574.78      google
853655.63       pepperjam
837677.41       Iterable
280968.25       attentive
221821.04       bing
*/






