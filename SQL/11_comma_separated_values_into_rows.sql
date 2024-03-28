--Convert Comma Separated Values into Rows
-- Find out rooms searched most number of times
-- Consider each unique room type as separate row
create table airbnb_searches 
(
user_id int,
date_searched date,
filter_room_types varchar(200)
);

insert into airbnb_searches values
(1,'2022-01-01','entire home,private room')
,(2,'2022-01-02','entire home,shared room')
,(3,'2022-01-02','private room,shared room')
,(4,'2022-01-03','private room')
;

--Solution - SQL
Select value as room_type, count(1) as no_of_searches from airbnb_searches
CROSS APPLY string_split(filter_room_types,',')
group by value
order by no_of_searches desc

-- Solution in Postgres
with cte_split_filter_rooms as
( select 
	user_id,
	date_searched,
	filter_room_types,
	unnest(string_to_array(filter_room_types, ',')) as split_filter_rooms
from airbnb_searches
)
select
	split_filter_rooms,
	count(split_filter_rooms) as count_filter_room_types
from cte_split_filter_rooms
group by split_filter_rooms
order by count_filter_room_types desc;