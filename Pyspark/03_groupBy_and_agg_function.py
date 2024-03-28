#Input
# col1 col2 col3
# a    aa   1
# a    aa   2 
# b    bb   5
# b    bb   3
# b    bb   4

#Output
# col1   col2   col3
# a       aa      [1,2]
# b       bb      [5,3,4]

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#Initialize Spark Session
spark = SparkSession.builder.appName("aggregate").getOrCreate()

sample_data = [ ("a","aa",1),
                ("a","aa",2),
                ("b","bb",5),
                ("b","bb",3),
                ("b","bb",4)]
columns = ["col1","col2","col3"]
df = spark.createDataFrame(sample_data, schema=columns)
df.show()
df_grouped = df.groupBy("col1","col2").agg(F.collect_list("co3").alias("col3"))
df_grouped.show()
spark.stop()

""""Method 2"""
df.createOrReplaceTempView("df_table")

result = spark.sql("""Select col1, col2, collect_list(col3) as col3 from df_table group by col1, col2
""")

result.show()

# Calculate max of average stock value per day, per stock
from pyspark.sql.functions import col 
from pyspark.sql import functions as F
data = [("2023-01-01", "AAPL", 150.00),
        ("2023-01-01", "AAPL", 200.00),
        ("2023-01-02", "AAPL", 155.00),
        ("2023-01-01", "GOOG", 2500.00),
        ("2023-01-01", "GOOG", 2800.00),
        ("2023-01-02", "GOOG", 2550.00),
        ("2023-01-01", "MSFT", 300.00),
        ("2023-01-02", "MSFT", 310.00),
]
schema = ["date", "stock", "value"]
df = spark.createDataFrame(data, schema = schema)
df1 = df.withColumn("date", F.to_date(col("date"),'yyyy-MM-dd'))
df1.show()
df_avg = df1.groupBy("date","stock").agg(F.avg("value").alias("avg_stock_value"))
df_avg.show()
df_max_avg = df_avg.groupBy("stock").agg(F.max("avg_stock_value").alias("max_avg_stock_value"))
df_max_avg.show()
spark.stop()