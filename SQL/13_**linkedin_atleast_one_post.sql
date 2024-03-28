-- Table 1 - Post_Info
-- Columns: Post_ID, user_id, post_date,category,count_likes

-- Question1. Identify users who have made at least one post every month for the last three month?

create table post_info (
  Post_ID int,
  user_id int,
  post_date date,
  category varchar(10),
  count_likes int
  );

   insert into post_info values 
  (1,1,'2022-01-01','A',1);
