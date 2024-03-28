
/*Print nth Monday after given input Date */

declare @input_date date;
declare @n int;
declare @days_to_add int;
declare @weekday varchar;
set @input_date = '2022-01-01'; --saturday
set @n = '3';
set @weekday = 'Monday';

set @days_to_add =  7 - datepart(weekday, @input_date) + 2;
--Here 2 = Monday
/*
1= Sunday
2=Monday
4=Wednesday
and so on 
*/
select @days_to_add as output;
-- Printing 1st Monday after input date
select dateadd(day,@days_to_add,@input_date) as output;
-- Printing nth Monday after input dateadd
select dateadd(week,@n-1,dateadd(day,@days_to_add,@input_date)) as nth_date;