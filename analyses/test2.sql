select *
from {{ ref('stg_products') }}
where product_id = 199052853261  and product_variant_id = 2066821316621

select 
row_number() over (partition by product_id, product_variant_id order by product_updated_at_ts desc, product_variant_updated_at_ts desc) as row_num,
*
from {{ ref('stg_products') }}
where product_id = 199052853261  and product_variant_id = 2066821316621
