# Question — Write a Pyspark code to find the output table as given below-
# employeeid,default_number,total_entry,total_login, total_logout, latest_login,latest_logout.

# The first step is to create two DataFrames called checkin_df and detail_df. The checkin_df DataFrame contains the following columns:
# employeeid: The employee ID
# entry_details: The type of entry (login or logout)
# timestamp_details: The timestamp of the entry

# The detail_df DataFrame contains the following columns:
# id: The employee ID
# phone_number: The employee’s phone number
# isdefault: A flag indicating whether the employee is a default user

# The next step is to join the two DataFrames on the employeeid column. This will create a new DataFrame called joined_df that contains all of the data from both DataFrames.
# The next step is to filter the joined_df DataFrame to only include rows where the isdefault column is equal to true. This will ensure that we only consider default users in our analysis.
# The next step is to create three separate DataFrames:
# total_entry_df: This df will contain the total number of entries for each employee.
# total_login_df: This df will contain the total number of logins for each employee.
# latest_login_df: This df will contain the latest login timestamp for each employee.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import col
spark = SparkSession.builder.master("local[*]").appName("Login").getOrCreate()
checkin_df = spark.createDataFrame([(1000, 'login', '2023-06-16 01:00:15.34'),
                                   (1000, 'login', '2023-06-16 02:00:15.34'),
                                   (1000, 'login', '2023-06-16 03:00:15.34'),
                                   (1000, 'logout', '2023-06-16 12:00:15.34'),
                                   (1001, 'login', '2023-06-16 01:00:15.34'),
                                   (1001, 'login', '2023-06-16 02:00:15.34'),
                                   (1001, 'login', '2023-06-16 03:00:15.34'),
                                   (1001, 'logout', '2023-06-16 12:00:15.34')],
                                  ["employeeid", "entry_details", "timestamp_details"])

detail_df = spark.createDataFrame([(1001, 9999, 'false'),
                                   (1001, 1111, 'false'),
                                   (1001, 2222, 'true'),
                                   (1003, 3333, 'false')],
                                  ["id", "phone_number", "isdefault"])

joined_df = checkin_df.join(detail_df,checkin_df['employeeid'] == detail_df['id'],'left')
joined_df = joined_df.filter(joined_df.isdefault == 'true')
joined_df.show()
total_entries = joined_df.groupBy('employeeid').agg(F.count('entry_details').alias('total_entries'))
total_entries.show()
total_logins = joined_df.filter(joined_df.entry_details=='login').groupBy('employeeid').agg(F.count('entry_details').alias('total_logins'))
total_logins.show()

windowSpec = Window.partitionBy("employeeid").orderBy(col("timestamp_details").desc())
login_df = joined_df.filter(joined_df.entry_details=='login').withColumn('login_rank',F.rank().over(windowSpec))
latest_login = login_df.filter(login_df.login_rank==1)
latest_login.show()

logout_df = joined_df.filter(joined_df.entry_details=='logout').withColumn('logout_rank',F.rank().over(windowSpec))
latest_logout = logout_df.filter(logout_df.logout_rank==1)
latest_logout.show()
latest_login = latest_login.select(latest_login.employeeid,latest_login.timestamp_details.alias("latest_login"))
latest_logout = latest_logout.select(latest_logout.employeeid,latest_logout.timestamp_details.alias("latest_logout"))
final_df = total_entries.join(total_logins,total_entries['employeeid']==total_logins['employeeid'],how='inner')
final_df = final_df.join(latest_login,on='employeeid',how='inner')
final_df = final_df.join(latest_logout,on='employeeid',how='inner')
final_df.show()

#employee count under each manager
data = [('4529', 'Nancy', 'Young', '4125'),
('4238','John', 'Simon', '4329'),
('4329', 'Martina', 'Candreva', '4125'),
('4009', 'Klaus', 'Koch', '4329'),
('4125', 'Mafalda', 'Ranieri', 'NULL'),
('4500', 'Jakub', 'Hrabal', '4529'),
('4118', 'Moira', 'Areas', '4952'),
('4012', 'Jon', 'Nilssen', '4952'),
('4952', 'Sandra', 'Rajkovic', '4529'),
('4444', 'Seamus', 'Quinn', '4329')]
schema = ['employee_id' ,'first_name', 'last_name', 'manager_id']
df = spark.createDataFrame(data=data, schema=schema)

#using Spark-SQL
df.createOrReplaceTempView('EMP')
query = '''select  e.manager_id as manager_id, 
count(e.employee_id) as no_of_emp,(m.First_name) as mangr_name 
from  emp e
inner join emp m 
on m.employee_id =e.manager_id
group by 1,3
'''
result=spark.sql(query).show()

#Using dataframe

# Self-join the DataFrame to get manager names
result_df = df.alias("e").join(df.alias("m"), 
col("e.manager_id") == col("m.employee_id"), "inner").select(col("e.employee_id"), col("e.first_name"), col("e.last_name"),
col("e.manager_id"),col("m.first_name").alias("manager_first_name"))

result_df = df.alias("e").join(df.alias("e"),
                col("e.manager_id") == col("m.employee_id"), how="inner").select(
                            col("e.employee_id"), col("e.first_name"), col("e.last_name"),
                                col("e.manager_id"),col("m.first_name").alias("manager_first_name"))

result_df.show()
result_df.groupBy("manager_id", "manager_first_name").count().show()
