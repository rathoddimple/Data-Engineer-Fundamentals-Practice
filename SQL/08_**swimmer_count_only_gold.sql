-- Write a query to find number of gold medals per swimmer for swimmers who have only won gold medals

  
CREATE TABLE events (
ID int,
event varchar(255),
YEAR INt,
GOLD varchar(255),
SILVER varchar(255),
BRONZE varchar(255)
);

INSERT INTO events VALUES (1,'100m',2016, 'Amthhew Mcgarray','donald','barbara');
INSERT INTO events VALUES (2,'200m',2016, 'Nichole','Alvaro Eaton','janet Smith');
INSERT INTO events VALUES (3,'500m',2016, 'Charles','Nichole','Susana');
INSERT INTO events VALUES (4,'100m',2016, 'Ronald','maria','paula');
INSERT INTO events VALUES (5,'200m',2016, 'Alfred','carol','Steven');
INSERT INTO events VALUES (6,'500m',2016, 'Nichole','Alfred','Brandon');
INSERT INTO events VALUES (7,'100m',2016, 'Charles','Dennis','Susana');
INSERT INTO events VALUES (8,'200m',2016, 'Thomas','Dawn','catherine');
INSERT INTO events VALUES (9,'500m',2016, 'Thomas','Dennis','paula');
INSERT INTO events VALUES (10,'100m',2016, 'Charles','Dennis','Susana');
INSERT INTO events VALUES (11,'200m',2016, 'jessica','Donald','Stefeney');
INSERT INTO events VALUES (12,'500m',2016,'Thomas','Steven','Catherine');

-- ID	event	YEAR	GOLD	SILVER	BRONZE
-- 1	100m	2016	Amthhew Mcgarray	donald	barbara
-- 2	200m	2016	Nichole	Alvaro Eaton	janet Smith
-- 3	500m	2016	Charles	Nichole	Susana
-- 4	100m	2016	Ronald	maria	paula
-- 5	200m	2016	Alfred	carol	Steven
-- 6	500m	2016	Nichole	Alfred	Brandon
-- 7	100m	2016	Charles	Dennis	Susana
-- 8	200m	2016	Thomas	Dawn	catherine
-- 9	500m	2016	Thomas	Dennis	paula
-- 10	100m	2016	Charles	Dennis	Susana
-- 11	200m	2016	jessica	Donald	Stefeney
-- 12	500m	2016	Thomas	Steven	Catherine

-- Output
-- swimmer	count(e.year)
-- Amthhew Mcgarray	1
-- Charles	3
-- Ronald	1
-- Thomas	3
-- jessica	1
  
with gold_swimmers as 
(Select distinct(gold) as swimmer
  from events 
  where gold not in 
  (Select distinct(silver) as swimmer from events
   Union
   Select distinct(bronze) as swimmer from events
   )
 )
 Select e.gold as swimmer, count(e.year)
 from events e
 join gold_swimmers g
 on e.gold = g.swimmer
 group by e.gold


