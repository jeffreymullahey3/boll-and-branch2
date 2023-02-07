with orders as (

    select * from {{ ref('stg_orders') }}
),

products as (

    select * from {{ ref('stg_products') }}
),

orders_with_products as (

    select 
    
        orders.order_surrogate_primary_key,
        orders.order_surrogate_foreign_key,

        orders.order_id,
        
        orders.price,
        orders.quantity,
        orders.line_total_discount,

        products.product_id,
        products.product_category,
    
        products.product_title,
        
        products.product_variant_id,
        products.product_variant_sku,
        products.product_variant_title,
        
        products.product_variant_option1,
        products.product_variant_option2

    from orders
    left join products
    on orders.order_surrogate_foreign_key = products.products_surrogate_primary_key
)

select * from orders_with_products
order by order_id, product_id, product_variant_id 