/*Union vs Union All */
/*
Both UNION and UNION ALL combine the result of two or more tables. 
The result set of UNION does not contain duplicate rows, while the result set of UNION ALL returns all the rows
from both tables. The execution time of UNION ALL is less than the execution time of UNION as it does not 
remove the duplicate rows
*/

/*Group_Concat, String_Agg*/
/* Aggregates data of multiple rows into one column - Use with group by*/
/*

Syntax
MY_SQL
group_concat(distinct(column_name), ',')

Postgres / SQL
string_agg(column_name,',')
-- column_name, custom_separator

*/

/*Splitting Column into Rows*/
/* Post gres
unnest(string_to_array(column_name, ',')) as new_column_name
*/
/* SQL 
string_split(column_name,,',')
*/