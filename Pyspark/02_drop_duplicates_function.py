#Read parquet from datalake, drop duplicates and write it back to datalake


from pyspark.sql import SparkSession
from pyspark.sql import *

#Initialize Spark Session
spark = SparkSession.builder.appName('test').getOrCreate()

source = "s3://prefix/file_name.parquet"
target = source 

parquet_df = spark.read.parquet(source,inferSchema=True)
parquet_df_no_duplicate = parquet_df.dropDuplicates()
parquet_df_no_duplicate.show()
parquet_df.write.parquet(target,mode="overwrite")
spark.stop()