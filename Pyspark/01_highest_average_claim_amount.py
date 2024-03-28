#Consider two PySpark DataFrames - 'policy_data' and 'claim_data.' 
#The 'policy_data' DataFrame has columns: 'policy_id,' 'customer_id,' 'premium_amount,' and 'coverage_amount.' 
#The 'claim_data' DataFrame has columns: 'claim_id,' 'policy_id,' 'claim_amount,' and 'approval_status.' 
#Write a PySpark script to analyze these datasets and find the average claim amount per policy for each customer. 
#Display the customer with the highest average claim amount.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#Initialize Spark Session
spark = SparkSession.builder.master("local").getOrCreate()
#Read CSVs
policy_df = spark.read.csv('./policy_data.csv', header=True, inferSchema=True)
claims_df = spark.read.csv('./claims_data.csv', header=True, inferSchema=True)
# Join the datasets on the 'policy_id' column
joined_df = policy_df.join(claims_df, policy_df.policy_id == claims_df.policy_id, "inner")
joined_df.show()
# Calculate the claim amount per policy for each claim
joined_df = joined_df.withColumn('claim_amount_per_policy', F.col('claim_amount'))
# Calculate the average claim amount per policy for each customer
avg_claim_df = joined_df.groupBy('customer_id').agg(F.avg('claim_amount_per_policy').alias('avg_claim_amount_per_policy'))
# Find the customer with the highest average claim amount
max_avg_claim_customer = avg_claim_df.orderBy(F.col("avg_claim_amount_per_policy").desc())
print(f"Highest average claim customer - {max_avg_claim_customer.first()['customer_id']}")
#or max_avg_claim_customer.head(1)


