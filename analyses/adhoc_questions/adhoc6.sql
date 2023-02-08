select count(distinct first_user_id) as c1,
utm_campaign
from {{ ref('fct_web_events') }}
where utm_campaign is not null
group by 2
order by 1 desc 
/*
c1      utm_campaign
44033   g|search|brand|core|desktop|exact
30772   Boll&Branch_Prospecting_April2022_Broad+Lookalike_Influencer
15987   73861
11626   Boll&Branch_Prospecting_May2022_ContentCollective_Brand
11033   instagram_halfbakedharvest
10832   g|search|brand|plus|desktop|exact
*/
