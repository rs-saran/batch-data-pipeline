import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Initialize Spark session
spark = (
    SparkSession.builder.appName("Unique Dates Retrieval")
    .config("spark.sql.extensions", "delta.sql.DeltaSparkSessionExtensions")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.region", "us-east-1")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

# Path to the Delta table for consumption_end events
delta_table_path = (
    "s3a://processed/clickstream_data_dlh/consumption_end/delta_table"
)

# Read the Delta table
consumption_end_df = spark.read.format("delta").load(delta_table_path)

# Extract unique dates from eventtimestamp
unique_dates = consumption_end_df.select(
    F.to_date("eventtimestamp").alias("event_date")
).distinct()

# Show the unique dates
print("Unique Dates for Consumption Start Events:")
unique_dates.show()
