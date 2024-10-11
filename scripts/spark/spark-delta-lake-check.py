from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Unique Dates Retrieval") \
    .config("spark.sql.extensions", "delta.sql.DeltaSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Path to the Delta table for consumption_end events
delta_table_path = "s3a://processed/clickstream_data_dlh/consumption_start/delta_table"

# Read the Delta table
consumption_end_df = spark.read.format("delta").load(delta_table_path)

# Extract unique dates from eventtimestamp
unique_dates = consumption_end_df.select(F.to_date("eventtimestamp").alias("event_date")).distinct()

# Show the unique dates
print("Unique Dates for Consumption Start Events:")
unique_dates.show()
