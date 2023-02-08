
 select *
 from {{ ref('stg_web_events') }}
 where web_event_ts is null
-- none


  select *
 from {{ ref('stg_web_events') }}
 where event_url is null
 -- lots

  select *
 from {{ ref('stg_web_events') }}
 where is_new_session = 1 and web_event_ts is not null and event_url is null
 -- lots