from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#Initialize Spark Session
spark = SparkSession.builder.master("local").getOrCreate()
sample_df = spark.read.csv('./claims_data.csv',header=True,inferSchema=True)

data = [("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)]
df = spark.createDataFrame(data,schema = ["date","increment"])
df.show()


##Increment month of the date
#selectExpr() is a function of DataFrame that is similar to select()
#the difference is it takes a set of SQL expressions in a string to execute. 
#This gives the ability to run SQL like expressions without creating a temporary table and views.
# using selectExpr()
df.selectExpr("date","increment","add_months(to_date(date,'yyyy-MM-dd'),cast(increment as int)) as inc_date").show()

#Same as above using select() and expr()
# using select() and expr() 
from pyspark.sql.functions import expr, col
df.select(col("date"),col("increment"), \
          expr("add_months(to_date(date,'yyyy-MM-dd),cast(increment as int))").alias("inc_date")).show()
