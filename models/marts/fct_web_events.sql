with web_events as (

    select *
    
     from {{ ref('int_web_events') }}
),

products as (

    select 
        product_id,
        product_category,
        product_title,
    
        row_number() over (partition by product_id order by product_updated_at_ts desc) as row_num
    
     from {{ ref('stg_products') }}
     
),

products_distinct as (

    select 
        product_id,
        product_category,
        product_title
    
     from products
     where row_num = 1
     
),

web_events_with_products as (

    select 
    
        web_events.*,
        
        products_distinct.product_category,
    
        products_distinct.product_title


    from web_events
    left join products_distinct
    on web_events.product_id = products_distinct.product_id
)

select * from web_events_with_products
order by cookie_id_formatted, web_event_ts