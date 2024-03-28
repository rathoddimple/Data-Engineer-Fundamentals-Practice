-- Table: Transactions

-- +---------------+---------+
-- | Column Name   | Type    |
-- +---------------+---------+
-- | id            | int     |
-- | country       | varchar |
-- | state         | enum    |
-- | amount        | int     |
-- | trans_date    | date    |
-- +---------------+---------+
-- id is the primary key of this table.
-- The table has information about incoming transactions.
-- The state column is an enum of type ["approved", "declined"].
 

-- Write an SQL query to find for each month and country, the number of transactions and their total amount, the number of approved transactions and their total amount.

-- Return the result table in any order.

-- The query result format is in the following example.

 

-- Example 1:

-- Input: 
-- Transactions table:
-- +------+---------+----------+--------+------------+
-- | id   | country | state    | amount | trans_date |
-- +------+---------+----------+--------+------------+
-- | 121  | US      | approved | 1000   | 2018-12-18 |
-- | 122  | US      | declined | 2000   | 2018-12-19 |
-- | 123  | US      | approved | 2000   | 2019-01-01 |
-- | 124  | DE      | approved | 2000   | 2019-01-07 |
-- +------+---------+----------+--------+------------+
-- Output: 
-- +----------+---------+-------------+----------------+--------------------+-----------------------+
-- | month    | country | trans_count | approved_count | trans_total_amount | approved_total_amount |
-- +----------+---------+-------------+----------------+--------------------+-----------------------+
-- | 2018-12  | US      | 2           | 1              | 3000               | 1000                  |
-- | 2019-01  | US      | 1           | 1              | 2000               | 2000                  |
-- | 2019-01  | DE      | 1           | 1              | 2000               | 2000                  |
-- +----------+---------+-------------+----------------+--------------------+-----------------------+


with cte_month as (
select *, to_char(trans_date,'YYYY-MM') as month from transactions
),
total_trans as (
Select month, country, count(trans_date) as trans_count, sum(amount) as trans_total_amount
from cte_month
group by month,country
),
approved_trans as (
Select month, country, state, case when state ='approved' then count(1) else 0 end as approved_count,
case when state ='approved' then sum(amount) else 0 end as approved_total_amount
from cte_month
group by month, country, state, amount
),
approved_trans_final as 
(
    select month, country, sum(approved_count) as approved_count, sum(approved_total_amount) as approved_total_amount from approved_trans
    group by month, country
)
--NULL condition to handle joining nulls in country value
Select t.month, t.country, t.trans_count, a.approved_count, t.trans_total_amount, a.approved_total_amount
from total_trans t join approved_trans_final a
on t.month = a.month where ((t.country = a.country) or t.country IS NULL and a.country IS NULL)


select round((sum(case when order_date =customer_pref_delivery_date then 1 else 0 end))*100/
                (select count(distinct(customer_id)) from delivery)    )
              as immediate_percentage
from delivery

