
/*
create table customer_orders (
order_id integer,
customer_id integer,
order_date date,
order_amount integer
);
*/
/*
insert into customer_orders values(1,100,'2022-01-01',2000),(2,200,'2022-01-01' ,2500),(3,300,'2022-01-01',2100),(4,100,'2022-01-02',2000),(5,400,'2022-01-02',2200),(6,500,'2022-01-02',2700),(7,100,'2022-01-03',3000),(8,400,'2022-01-03',1000),(9,600,'2022-01-03',3000);
*/
/*
select * from customer_orders
*/

/*
order_id	customer_id	order_date	order_amount
1	        100	        022-01-01	2000
2	        200	        2022-01-01	2500
3	        300 	    2022-01-01	2100
4	        100	        2022-01-02	2000
5	        400	        2022-01-02	2200
6	        500	        2022-01-02	2700
7	        100	        2022-01-03	3000
8	        400	        2022-01-03	1000
9	        600	        2022-01-03	3000
*/

with customer_cte as (
select customer_id, order_date, lag(order_date) over(partition by customer_id) as lagged_date
from customer_orders
  )
select order_date, sum(case when lagged_date is not NULL or lagged_date !='' then 1 else 0 end) as returning_customers, sum(case when lagged_date is NULL or lagged_date ='' then 1 else 0 end) as new_customers from customer_cte
group by order_date 
