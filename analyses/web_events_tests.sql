
select 
        web_event_id,
        web_event_loaded_at,
        cookie_id_raw,
        cookie_id_formatted,
        customer_id,
        web_event_name,
        web_event_properties,
        web_event_ts,
        
        global_session_id,
        cookie_session_id,

        utm_campaign,
        utm_source,
        utm_medium

from {{ ref('stg_web_events') }}
where cookie_id_formatted = 'd063b426-6f99-49cc-a38a-78e3bcd27ff5'
order by web_event_ts






select 
        event_name,
        event_properties,

        case when lower(event_name) = 'order_completed' then RTRIM(SUBSTR(event_properties, 13), '}') else ' ' end as order_id,

        case when lower(event_name)  IN ('product_viewed', 'product_added') then RTRIM(SUBSTR(event_properties, 16), '"}') else ' ' end as product_id,
        safe_cast((case when lower(event_name) IN ('product_viewed', 'product_added') then RTRIM(SUBSTR(event_properties, 16), '"}') else '' end) AS INT64) as product_id2,

        utm_campaign,
        utm_source,
        utm_medium

from {{ source('ae_data_challenge', 'web_events1') }}

where cookie_id = 'd063b426-6f99-49cc-a38a-78e3bcd27ff5'
order by timestamp

