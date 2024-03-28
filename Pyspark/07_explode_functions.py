
# 1. Explode Function
# +------+---------+----------+-----------+---------+
# |EmpId |Name     | Locations
# +------+---------+----------+-----------+---------+
# |1     |Gaurav   | Pune     , Bangalore, Hyderabad|
# |2     |Risabh   | Mumbai   ,Bangalore, Pune      |
# +------+---------+----------+-----------+---------+
 
# Required Output
# EmpId Name Location
# 1     Gaurav  Pune
# 1     Gaurav  Bangalore
# 1     Gaurav  Hyderabad
# 2     Risabh  Mumbai
# 2     Risabh  Pune

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[*]").appName("Explode").getOrCreate()
data = [Row(1,"Gaurav",["Pune","Bangalore","Hyderabad"]),
        Row(2,"Rishabh",["Mumbai","Bangalore","Pune"])]

schema = StructType([StructField("Empid",IntegerType(),nullable=False),
                    StructField("Name",StringType(),nullable=False),
                    StructField("Location",ArrayType(StringType()),nullable=False)])

df = spark.createDataFrame(data=data,schema=schema)
df.show()
df.printSchema()

df_exploded = df.select(df["Empid"],df["Name"],F.explode(df["Location"]).alias("Location"))
df_exp = df.select("Name", F.explode("Location").alias("location"))