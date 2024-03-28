-- Table - Movie_Seats
-- Columns - Seat_no, Occupant_gender
-- Task - Design a SQL code to see if there are more than 3 consecutive males sitting together.
create table movie_seats
(
seat_no int,
occupant_gender varchar(2)
);

insert into movie_seats values
(1,'m')
,(2,'f')
,(3,'m')
,(4,'f')
,(5,'f')
,(6,'m')
,(7,'m')
,(8,'m')
,(9,'m')
,(10,'m')
,(11,'f')
,(12,'m')
,(13,'m')
,(14,'m')
,(15,'f')
;


with diff_bw_seats as (
  Select seat_no, occupant_gender, row_number() over (order by seat_no) as rn,
  row_number() over(partition by occupant_gender order by seat_no) as rn_gender from movie_seats),
  grouped_seats as (
 select *, seat_no - rn_gender as groupnum from diff_bw_seats ),
 consecutive_males as (
 select groupnum, min(seat_no), max(seat_no), case when max(seat_no) - min(seat_no) >3 then 1 else 0 end as consecutive_3_plus_males from grouped_seats where occupant_gender='m'
 group by groupnum)
 select sum(consecutive_3_plus_males) as number_of_3_plus_consecutive_males from consecutive_males
------------------------
WITH RankedSeats AS (
 SELECT
 Seat_no,
 Occupant_gender,
 Seat_no - ROW_NUMBER() OVER (
 PARTITION BY Occupant_gender
 ORDER BY Seat_no
 ) AS GroupID
 FROM
 Movie_Seats
 WHERE
 Occupant_gender = 'm'
)
SELECT
 GroupID,
 COUNT(*) AS MaleCount
FROM
 RankedSeats
GROUP BY
 GroupID
HAVING
 COUNT(*) > 3;