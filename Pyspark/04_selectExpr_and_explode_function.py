# Input
# json_file = 
# {"dept_id":101, "e_id": [10101,10102,10103]}
# {"dept_id":102, "e_id": [10201,10202]}

# Output
# dept_id e_id
# 101     10101
# 101     10102
# 101     10103
# 102     10201
# 102     10202

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#Initialize Spark Session
spark = SparkSession.builder.appName("json_explode").getOrCreate()

df = spark.read.json(json_file_path)

#selectExpr() is a function of DataFrame that is similar to select()
# the difference is it takes a set of SQL expressions in a string to execute. 
# This gives the ability to run SQL like expressions without creating a temporary table and views.
#selectExpr() just has one signature that takes SQL expression in a String and returns a new DataFrame. 
# Note like select() it doesn’t have a signature to take the Column type.


df_exploded = df.selectExpr("dept_id", F.explode("e_id").alias("e_id"))
df_exploded.show()
spark.stop()


