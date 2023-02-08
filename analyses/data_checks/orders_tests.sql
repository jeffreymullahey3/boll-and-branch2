select _id,
_loaded_at,
created_at,
updated_at,
subtotal,
total,
l.line_id,
l.product_id,
l.variant_id,
l.price,
l.quantity,
l.line_total_discount
from bb-dc-1.test.orders1, UNNEST(line_items) as l



select 
row_number() over (partition by order_id, product_id order by order_updated_at_ts desc) as row_num,
*
from {{ ref('stg_orders') }}


select *
from {{ ref('stg_orders') }}
where order_id = 'B1787532'
