with web_events as (

    select *

    from {{ source('ae_data_challenge', 'web_events1') }}
)

select * from web_events