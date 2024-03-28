/*
create table entries ( 
name varchar(20),
address varchar(20),
email varchar(20),
floor int,
resources varchar(10));
*/

/*
insert into entries 
values ('A','Bangalore','A@gmail.com',1,'CPU'),('A','Bangalore','A1@gmail.com',1,'CPU'),('A','Bangalore','A2@gmail.com',2,'DESKTOP')
,('B','Bangalore','B@gmail.com',2,'DESKTOP'),('B','Bangalore','B1@gmail.com',2,'DESKTOP'),('B','Bangalore','B2@gmail.com',1,'MONITOR')
*/
--select * from entries

-- name	address	email	floor	resources
-- A	Bangalore	A@gmail.com	1	CPU
-- A	Bangalore	A1@gmail.com	1	CPU
-- A	Bangalore	A2@gmail.com	2	DESKTOP
-- B	Bangalore	B@gmail.com	2	DESKTOP
-- B	Bangalore	B1@gmail.com	2	DESKTOP
-- B	Bangalore	B2@gmail.com	1	MONITOR

/*
create table entries ( 
name varchar(20),
address varchar(20),
email varchar(20),
floor int,
resources varchar(10));
*/

/*
insert into entries 
values ('A','Bangalore','A@gmail.com',1,'CPU'),('A','Bangalore','A1@gmail.com',1,'CPU'),('A','Bangalore','A2@gmail.com',2,'DESKTOP')
,('B','Bangalore','B@gmail.com',2,'DESKTOP'),('B','Bangalore','B1@gmail.com',2,'DESKTOP'),('B','Bangalore','B2@gmail.com',1,'MONITOR')
*/
--select * from entries

/* Expected Output
name	total_visits	floor	resources_used
A	3	1	CPU,DESKTOP
B	3	2	DESKTOP,MONITOR
*/

with floor_data as (
Select name, floor, count(floor), row_number() over(partition by name order by count(floor) desc) as rn
  from entries
group by name, floor 
  ),
  total_floor_data as (
     select name, count(floor) as total_visits, group_concat(DISTINCT(resources)) as resources_used from entries group by name)
    select f.name, t.total_visits, f.floor, t.resources_used
    from floor_data f join total_floor_data t
    on f.name = t.name
    where f.rn = 1


