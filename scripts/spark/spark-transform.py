from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, IntegerType, LongType, TimestampType, TimestampNTZType
import boto3
import sys

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Flatten Parquet") \
    .getOrCreate()

def flatten_schema(df):
    """
    Flattens the schema of a DataFrame.
    """
    exploded_df = df.select(
                  "event_name",
                  "eventtimestamp",
                  "userid",
                  "platform",
                  "sessionid",
                  "appversion",
                  F.explode("event_specific_properties").alias("key", "value")
                  )
    flattened_df = exploded_df.groupBy(
                    "event_name", 
                    "eventtimestamp", 
                    "userid", 
                    "platform", 
                    "sessionid", 
                    "appversion"
                  ).pivot("key").agg(F.first("value"))
                    

    return flattened_df




def main(input_bucket="clickstream/consumption_end/2023/10/1", input_file="events.parquet", output_bucket="processed", output_file="processed.parquet"):
    schema = StructType([
                StructField("event_name", StringType(), True),
                StructField("eventtimestamp", TimestampNTZType(), True),
                StructField("userid", StringType(), True),
                StructField("platform", StringType(), True),
                StructField("sessionid", StringType(), True),
                StructField("appversion", StringType(), True),
                StructField("event_specific_properties", BinaryType(), True)
            ])
    # Read Parquet file from MinIO
    df = spark.read.schema(schema).parquet(f's3a://{input_bucket}/{input_file}')

    # Flatten the schema
    flattened_df = flatten_schema(df)

    # Write the flattened DataFrame back to MinIO as Parquet
    flattened_df.write.parquet(f's3a://{output_bucket}/{output_file}')

if __name__ == "__main__":
    # if len(sys.argv) != 5:
    #     print("Usage: process_parquet.py <input_bucket> <input_file> <output_bucket> <output_file>")
    #     sys.exit(1)

    # input_bucket = sys.argv[1]
    # input_file = sys.argv[2]
    # output_bucket = sys.argv[3]
    # output_file = sys.argv[4]

    # MinIO connection
    # minio_client = Minio(
    #     "minio:9000",
    #     access_key="minio",
    #     secret_key="minio123",
    #     secure=False
    # )

    main()

    spark.stop()
