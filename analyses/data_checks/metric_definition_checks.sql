-- Total Bounced Web Sessions: count of web sessions where total `page` events is less than or equal to 1
with total_bounced as (
    select count(cookie_id_formatted) as total_bounced
    from 
    (
        select count(*) as c1,
        cookie_id_formatted,
        cookie_session_id
        from {{ ref('fct_web_events') }}
        where web_event_name = 'page'
        group by 2,3
        having c1 <= 1
    )


-- Product View Rate: Total web sessions that include a `product_viewed` event divided by Total Web Sessions
select distinct cookie_id_formatted,
cookie_session_id
from {{ ref('fct_web_events') }}
where web_event_name = 'product_viewed'