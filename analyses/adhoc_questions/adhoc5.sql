select count(*) as c1,
prior_url
from (
    select 
    LAG(event_url,1) OVER   
    (PARTITION BY cookie_id_formatted ORDER BY web_event_ts) AS prior_url,
    *
from {{ ref('fct_web_events') }}
where lower(web_event_name) = 'page' and event_url like '%checkout.bollandbranch.com%'
)
group by 2
order by 1 desc

/*
c1      prior_url
47019   NULL
3289    https://checkout.bollandbranch.com/account/login?return_url=%2Faccount
1919    https://checkout.bollandbranch.com/account
1317    https://checkout.bollandbranch.com/account/login
1146    https://checkout.bollandbranch.com/account/register
175     https://checkout.bollandbranch.com/account/invalid_token