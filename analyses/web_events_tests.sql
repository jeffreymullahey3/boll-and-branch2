
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
        web_event_name,
        web_event_properties,

        case when lower(web_event_name) = 'order_completed' then RTRIM(SUBSTR(web_event_properties, 13), '}') else ' ' end as order_id,

        case when lower(web_event_name)  IN ('product_viewed', 'product_added') then RTRIM(SUBSTR(web_event_properties, 16), '"}') else ' ' end as product_id,

        utm_campaign,
        utm_source,
        utm_medium

from {{ ref('stg_web_events') }}

where cookie_id = 'd063b426-6f99-49cc-a38a-78e3bcd27ff5'
order by web_event_ts

