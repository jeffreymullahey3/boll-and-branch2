select distinct
product_style
from {{ ref('fct_orders') }}
-- looks terrible


select distinct
product_size
from {{ ref('fct_orders') }}

select count(*) as c1,
product_category
from {{ ref('fct_orders') }}
group by 2

select distinct product_category
from {{ ref('fct_orders') }}

select distinct product_title
from {{ ref('fct_orders') }}

