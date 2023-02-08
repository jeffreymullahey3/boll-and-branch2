select avg(is_sheets_king) as avg1
from (
    select 
        order_id,
        max(case when upper(product_category) = 'SHEET SETS' and upper(product_size) like '%KING%' then 1 else 0 end) as is_sheets_king
    from {{ ref('fct_orders') }}
    group by 1
)

-- 33.3%