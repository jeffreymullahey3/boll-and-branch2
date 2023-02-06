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
