
select _id,
_loaded_at,
cookie_id,
customer_id,
event_name,
--event_url,
timestamp
from bb-dc-1.test.web_events1
where cookie_id = 'd063b426-6f99-49cc-a38a-78e3bcd27ff5'
order by timestamp


/*
select 
--_id,
--_loaded_at,
cookie_id,
customer_id,
event_name,
--event_url,
timestamp
from bb-dc-1.test.web_events1
where customer_id = '5748557873211'
order by timestamp
*/

