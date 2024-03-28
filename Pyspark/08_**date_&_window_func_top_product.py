# Problem: You have a dataset of sales transactions, and you want to find the top-selling product for each month. Write a PySpark query to achieve this using window functions.

# Source Data:
# Assume you have a PySpark DataFrame named `sales_data` with the following columns:
# - `product_id` (String): The unique identifier for each product.
# - `sales_date` (Date): The date of the sale.
# - `sales_amount` (Double): The amount of the sale.
# Here's a sample of the source data:
# +-----------+-----------+------------+
# | product_id|sales_date |sales_amount|
# +-----------+-----------+------------+
# | A | 2023-01-15| 100.0 |
# | B | 2023-01-20| 150.0 |
# | A | 2023-02-10| 120.0 |
# | B | 2023-02-15| 180.0 |
# | C | 2023-03-05| 200.0 |
# | A | 2023-03-10| 250.0 |
# +-----------+-----------+------------+

# Expected Output:
# You should generate a result Data Frame that shows the top-selling product for each month. The result should have the following columns:
# - `month` (String): The month for which the top-selling product is calculated.
# - `top_product` (String): The product that had the highest total sales for that month.

# Here's the expected output for the provided sample data:
# +-------+------------+
# | month|top_product |
# +-------+------------+
# |2023-01| B |
# |2023-02| B |
# |2023-03| A |
# +-------+------------+

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import col
from datetime import datetime
from pyspark.sql import Window

spark = SparkSession.builder.master("local[*]").appName("Explode").getOrCreate()
data = [("A",datetime.strptime('2023-01-15','%Y-%m-%d'),100.0),("B",datetime.strptime('2023-01-20','%Y-%m-%d'),150.0),("A",datetime.strptime('2023-02-10','%Y-%m-%d'),120.0),("B",datetime.strptime('2023-02-15','%Y-%m-%d'),180.0),("C",datetime.strptime('2023-03-05','%Y-%m-%d'),200.0),("A",datetime.strptime('2023-03-10','%Y-%m-%d'),250.0)]
schema = StructType([StructField("product_id",StringType(),nullable=False),StructField("sales_date",DateType(),nullable=False),
                      StructField("sales_amount",DoubleType(),nullable=False)])
df = spark.createDataFrame(data=data,schema=schema)
df.show()
df_month = df.withColumn('sales_month',F.trunc('sales_date','MM'))
windowSpec = Window.partitionBy("sales_month").orderBy(col("sales_amount").desc())
df_rank = df_month.withColumn('sales_rank',rank().over(windowSpec))
df_highest_sales = df_rank.filter(df_rank.sales_rank==1).select(df_rank.sales_month.alias("month"),df_rank.product_id.alias("top_product"))
df_highest_sales = df_highest_sales.withColumn('month',F.date_format(df_highest_sales.month,'yyyy-MM'))
df_highest_sales.show()