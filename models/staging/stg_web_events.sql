

with format_cookie_id as (

            select *,
                trim(cookie_id, '\\"') as cookie_id_formatted
            from {{ source('ae_data_challenge', 'web_events1') }}
),

create_last_event as (

    select 
        *,

        LAG(timestamp,1) OVER (PARTITION BY cookie_id_formatted  ORDER BY timestamp) AS last_event

        from format_cookie_id


),

create_is_new_session as (

    select *,
        case when TIMESTAMP_DIFF(timestamp, last_event, SECOND) >= (60 * 30) or last_event is null then 1 else 0 end as is_new_session
    from create_last_event
),


add_session_id as (

select
*,

sum(is_new_session) over (order by cookie_id_formatted, timestamp) as global_session_id,
sum(is_new_session) over (partition by cookie_id_formatted order by timestamp) as cookie_session_id

from create_is_new_session
),


web_events as (

    select 
        _id as web_event_id,
        _loaded_at as web_event_loaded_at,
        cookie_id as cookie_id_raw,
        cookie_id_formatted,
        customer_id,
        event_name as web_event_name,
        event_url,
        event_properties as web_event_properties,

        CAST((case when lower(event_name) = 'order_completed' then RTRIM(SUBSTR(event_properties, 13), '}') else ' ' end) AS INT64) as order_id,

        CAST((case when lower(event_name) IN ('product_viewed', 'product_added') then RTRIM(SUBSTR(event_properties, 16), '"}') else ' ' end) AS INT64) as product_id,

        timestamp as web_event_ts,

        is_new_session,
        global_session_id,
        cookie_session_id,

        utm_campaign,
        utm_source,
        utm_medium


    from add_session_id
)

select * from web_events
--order by cookie_id_formatted, web_event_ts