
--80% Sales comes from 20% of products or services

with product_wise_sales as (
    select product_id, sum(sales) as product_sales
    from orders
),
calc_sales as (
select product_id, sum(product_sales) over(order by product_sales desc between unbounded preceeding and 0 preceeding) as running_sales,
0.8*sum(sales) over() as 80_percent_total_sales
from product_wise_sales
)
select * from calc_sales where running_sales <= 80_percent_total_sales