select sum(quantity) / count(distinct order_id) as average_order_units,
DATE_TRUNC(order_created_at_ts, DAY)
from {{ ref('fct_orders') }}
group by 2
order by 1 desc
-- 2022-08-05