
with 

add_landing_ts as (

    select cookie_id_formatted,
        cookie_session_id,
        web_event_ts
        
    from {{ ref('stg_web_events') }}
    where is_new_session = 1
),

add_landing_url1 as (

    select cookie_id_formatted,
        cookie_session_id,
        event_url,
        row_number() over (partition by cookie_id_formatted, cookie_session_id order by web_event_ts) as row_num,
        
    from {{ ref('stg_web_events') }}
    where event_url is not null
),

add_landing_url2 as (

    select cookie_id_formatted,
        cookie_session_id,
        event_url
        
    from add_landing_url1
    where row_num = 1
),


add_landing_medium1 as (

    select cookie_id_formatted,
        cookie_session_id,
        utm_medium,
        row_number() over (partition by cookie_id_formatted, cookie_session_id order by web_event_ts) as row_num,
        
    from {{ ref('stg_web_events') }}
    where utm_medium is not null
),

add_landing_medium2 as (

    select cookie_id_formatted,
        cookie_session_id,
        utm_medium
        
    from add_landing_medium1
    where row_num = 1
),

add_landing_source1 as (

    select cookie_id_formatted,
        cookie_session_id,
        utm_source,
        row_number() over (partition by cookie_id_formatted, cookie_session_id order by web_event_ts) as row_num,
        
    from {{ ref('stg_web_events') }}
    where utm_source is not null
),

add_landing_source2 as (

    select cookie_id_formatted,
        cookie_session_id,
        utm_source
        
    from add_landing_source1
    where row_num = 1
),


add_landing_campaign1 as (

    select cookie_id_formatted,
        cookie_session_id,
        utm_campaign,
        row_number() over (partition by cookie_id_formatted, cookie_session_id order by web_event_ts) as row_num,
        
    from {{ ref('stg_web_events') }}
    where utm_campaign is not null
),

add_landing_campaign2 as (

    select cookie_id_formatted,
        cookie_session_id,
        utm_campaign
        
    from add_landing_campaign1
    where row_num = 1
),



final as (
    select a.*,
    b.web_event_ts as landing_ts,
    c.event_url as landing_url,
    d.utm_medium as landing_utm_medium,
    e.utm_source as landing_utm_source,
    f.utm_campaign as landing_utm_campaign

    from {{ ref('stg_web_events') }} a

    left join add_landing_ts b on a.cookie_id_formatted = b.cookie_id_formatted and
        a.cookie_session_id = b.cookie_session_id

    left join add_landing_url2 c on a.cookie_id_formatted = c.cookie_id_formatted and
        a.cookie_session_id = c.cookie_session_id

    left join add_landing_medium2 d on a.cookie_id_formatted = d.cookie_id_formatted and
        a.cookie_session_id = d.cookie_session_id

    left join add_landing_source2 e on a.cookie_id_formatted = e.cookie_id_formatted and
        a.cookie_session_id = e.cookie_session_id

    left join add_landing_campaign2 f on a.cookie_id_formatted = f.cookie_id_formatted and
        a.cookie_session_id = f.cookie_session_id

)

select * from final