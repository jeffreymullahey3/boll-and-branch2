with products as (

    select 
        _id as product_id,
        _loaded_at as product_loaded_at_ts,
        category as product_category,
        
        a.created_at as product_created_at_ts,
        a.updated_at as product_updated_at_ts,
        a.title as product_title,

        v.variant_id as product_variant_id,
        v.sku as product_variant_sku,
        v.title as product_variant_title,
        v.created_at as product_variant_created_at_ts,
        v.updated_at as product_variant_updated_at_ts,
        v.option1 as product_variant_option1,
        v.option2 as product_variant_option2

    from {{ source('ae_data_challenge', 'products1') }} as a, unnest(variants) as v
)

select * from products