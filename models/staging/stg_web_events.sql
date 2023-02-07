with add_session_id as (

select
*,

sum(is_new_session) over (order by cookie_id, timestamp) as global_session_id,
sum(is_new_session) over (partition by cookie_id order by timestamp) as cookie_session_id

from (


    select *,
    case when TIMESTAMP_DIFF(timestamp, last_event, SECOND) >= (60 * 30) or last_event is null then 1 else 0 end as is_new_session

    from (

        select 
        *,
        LAG(timestamp,1) OVER (PARTITION BY cookie_id ORDER BY timestamp) AS last_event

        from {{ source('ae_data_challenge', 'web_events1') }}

        ) last
    ) final
),




web_events as (

    select 
        _id as web_event_id,
        _loaded_at as web_event_loaded_at,
        cookie_id,
        customer_id,
        event_name as web_event_name,
        event_url,
        event_properties as web_event_properties,

        case when lower(event_name) = 'order_completed' then RTRIM(SUBSTR(event_properties, 13), '}') else ' ' end as order_id,

        case when lower(event_name) IN ('product_viewed', 'product_added') then RTRIM(SUBSTR(event_properties, 16), '"}') else ' ' end as product_id,

        timestamp as web_event_ts,

        global_session_id,
        cookie_session_id,

        utm_campaign,
        utm_source,
        utm_medium


    from add_session_id
)

select * from web_events