create table icc_world_cup
(
Team_1 Varchar(20),
Team_2 Varchar(20),
Winner Varchar(20)
);
INSERT INTO icc_world_cup values('India','SL','India');
INSERT INTO icc_world_cup values('SL','Aus','Aus');
INSERT INTO icc_world_cup values('SA','Eng','Eng');
INSERT INTO icc_world_cup values('Eng','NZ','NZ');
INSERT INTO icc_world_cup values('Aus','India','India');
 
select * from icc_world_cup;

-- Team_1	Team_2	Winner
-- India	SL	India
-- SL	Aus	Aus
-- SA	Eng	Eng
-- Eng	NZ	NZ
-- Aus	India	India

with cte_matches as 
(Select team_1 as team, count(team_1) as match_played, sum(case when team_1 = winner then 1 else 0 end) as match_won
from icc_world_cup
group by team_1
union all
Select team_2 as team, count(team_2) as match_played, sum(case when team_2 = winner then 1 else 0 end) as match_won
from icc_world_cup
group by team_2)

select team, sum(match_played) as total_matches, sum(match_won) as matches_won,
sum(match_played) - sum(match_won) as matches_loss
from cte_matches group by team
order by matches_won desc
