with unnested_data as (

    select _id as order_id,
        --_loaded_at,
        created_at as order_created_at_ts,
        updated_at as order_updated_at_ts,
        subtotal as order_subtotal,
        total as order_total,
        --l.line_id,
        l.product_id,
        l.variant_id,
        l.price,
        l.quantity,
        l.line_total_discount
 from {{ source('ae_data_challenge', 'orders1') }}, UNNEST(line_items) as l

    
),

remove_dups as (


select *, 
row_number() over (partition by order_id, product_id order by order_updated_at_ts desc) as row_num,

from unnested_data
)

select 
        {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id', 'variant_id' ]) }} as order_surrogate_primary_key,
        {{ dbt_utils.generate_surrogate_key(['product_id', 'variant_id' ]) }} as order_surrogate_foreign_key,

        order_id,
        order_created_at_ts,
        order_updated_at_ts,
        order_subtotal,
        order_total,
        product_id,
        variant_id,
        price,
        quantity,
        line_total_discount,

        

from remove_dups
where row_num = 1