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
    LAG(timestamp,1) OVER
    (PARTITION BY cookie_id ORDER BY timestamp) AS last_event
from bb-dc-1.test.web_events1
where cookie_id = 'd063b426-6f99-49cc-a38a-78e3bcd27ff5'
) last
    ) final

