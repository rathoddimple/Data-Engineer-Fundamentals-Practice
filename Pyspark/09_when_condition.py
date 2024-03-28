from pyspark.sql.functions import when, col

# Define behavior-based segmentation conditions
segmentation_conditions = [
    (col('purchase_count') >= 10) & (col('total_spend') > 5000),  # High-value customers
    (col('purchase_count') >= 5) & (col('total_spend') > 1000),   # Mid-value customers
    (col('purchase_count') < 5) & (col('total_spend') <= 1000)    # Low-value customers
]

# Apply segmentation conditions using 'when' statement
segmentation = (
    df.withColumn('segment', 
                  when(segmentation_conditions[0], 'High-Value')
                  .when(segmentation_conditions[1], 'Mid-Value')
                  .when(segmentation_conditions[2], 'Low-Value')
                  .otherwise('Other'))
)

# Display the segmented customers
segmentation.show()