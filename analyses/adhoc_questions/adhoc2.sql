with totals as(
    
select sum(price*quantity) as total_revenue,
sum(line_total_discount) as line_total_discount,
product_variant_sku
from {{ ref('fct_orders') }}
group by 3
)

select *,
coalesce(total_revenue,0) - coalesce(line_total_discount,0) as gross_revenue
from totals
order by 2 desc
-- DSETKIS30HWT
