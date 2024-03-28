# Consider you are given inventory data to be cleaned 
# For productID column, if value exceeds 3 chars, remove any occurences chars (A-Za-z) that occur after and concatenate the remaining chars
# Add new col "availableFlag". If "inventoryFlag" is "Yes", assign value 1. Else check if same product has "inventoryFlag" as "Yes" for prev "weekOfYear", if does, assign value 1 else 0.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[*]").appName("Window_Func").getOrCreate()
Inventory = [ ("C1", "P1_PP2023", "Yes", 20, 202305), ("C1", "P2", "Yes", 21, 202305), ("C1", "P3", "Yes", 21, 202305),  ("C1", "P4", "Yes", 21, 202305), ("C1", "P1_2023", "No", 21, 202306), ("C1", "P3", "No", 22, 202306), ("C1", "P3", "No", 23, 202306)]
df = spark.createDataFrame(Inventory, ["compId","productId","inventoryFlag", "weekOfYear","yearMonth"])


df1 = df.withColumn("productId", F.concat(F.substring("productId",1,3), 
                                           F.regexp_replace(F.expr("substring(productId,4,length(productId))"),"[A-Za-z]","")))

df1.show()
windowSpec = Window.partitionBy("productId").orderBy("weekOfYear")
df2 = df1.withColumn("prevWeekInventoryFlag", F.lag("inventoryFlag").over(windowSpec))
df2.show()
df3  = df2.withColumn("prevWeekOfYear", F.lag("weekOfYear").over(windowSpec))
df3.show()


df4 = df3.withColumn("availableFlag", F.when(F.col("inventoryFlag") == 'Yes', 1).otherwise(
        F.when((F.col("prevWeekInventoryFlag") == 'Yes') &
               (F.col("prevWeekOfYear") == F.col("weekOfYear") - 1), 1)
            .otherwise(0)))
df4.show(truncate=False)
spark.stop()
