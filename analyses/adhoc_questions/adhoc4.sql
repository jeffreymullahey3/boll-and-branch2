-- kind of clunky, but you have to run part 1 first, then run part 2 second, and divide part 1 by part 2 to get the answer 

-- part 1

select count(*) as c1
from 
    (
        select distinct cookie_id_formatted, 
        cookie_session_id
        from {{ ref('fct_web_events') }}
where web_event_name = 'product_added' and upper(product_title) = 'PLUSH BATH TOWEL SET'
) -- 798




-- part 2


with table1 AS
    (
        select distinct 
            cookie_id_formatted, 
            cookie_session_id,
            order_id 
        from {{ ref('fct_web_events') }}
        where web_event_name = 'order_completed'
),

table2 as (
    select order_id,
        max(case when upper(product_title) = 'PLUSH BATH TOWEL SET' then 1 else 0 end) as has_plush_bath_towel_set
    from {{ ref('fct_orders') }}
    group by 1
),

table3 as (
select distinct cookie_id_formatted,
cookie_session_id
from table1 a
inner join (select order_id from table2 where has_plush_bath_towel_set = 1) b 
on a.order_id = b.order_id 
)

select count(*) as c1 from table3
-- 398

-- 398/798 = 49.9%
