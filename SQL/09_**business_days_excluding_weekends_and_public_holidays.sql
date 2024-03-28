--find difference between 2 dates excluding weekends and public holidays
--Basically we need to find business days between 2 given dates using SQL

create table tickets
(
ticket_id varchar(10),
create_date date,
resolved_date date
);

insert into tickets values
(1,'2022-08-01','2022-08-03')
,(2,'2022-08-01','2022-08-12')
,(3,'2022-08-01','2022-08-16');


create table holidays
(
holiday_date date
,reason varchar(100)
);

insert into holidays values
('2022-08-11','Rakhi'),('2022-08-15','Independence day');


-- Solution
with differences_in_no_of_days as (
Select ticket_id, create_date, resolved_date, ABS(EXTRACT(DAY FROM resolved_date::TIMESTAMP - create_date::TIMESTAMP)) as days_diff,
2*abs(TRUNC(DATE_PART('day', resolved_date::TIMESTAMP - create_date::TIMESTAMP)/7)) as weekends, 
case when date_part('isodow',holiday_date) = 6 or date_part('isodow',holiday_date)=7 or holiday_date is NULL then 0 else 1 END
as holiday_not_a_weekend,
ABS(EXTRACT(DAY FROM resolved_date::TIMESTAMP - create_date::TIMESTAMP)) -
2*abs(TRUNC(DATE_PART('day', resolved_date::TIMESTAMP - create_date::TIMESTAMP)/7)) 
- (case when date_part('isodow',holiday_date) = 6 or date_part('isodow',holiday_date)=7 or holiday_date is NULL then 0 else 1 END) as business_days,
holiday_date
     from tickets t left join holidays h 
     on h.holiday_date between t.create_date and t.resolved_date

) 
select ticket_id, create_date, resolved_date, days_diff - weekends - sum(holiday_not_a_weekend) as number_of_business_days
from differences_in_no_of_days
group by ticket_id, create_date, resolved_date, datediff, sat_sun
order by 4

     