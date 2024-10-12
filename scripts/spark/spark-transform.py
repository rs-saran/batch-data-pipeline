from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, MapType, TimestampType
import boto3
import sys
from delta.tables import DeltaTable

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




def main(event_name="consumption_end", event_date="2023/10/1"):
    print("\n\n----------------------MAIN-----------------------\n\n")
    schema = StructType([
                StructField("event_name", StringType(), True),
                StructField("eventtimestamp", TimestampType(), True),
                StructField("userid", StringType(), True),
                StructField("platform", StringType(), True),
                StructField("sessionid", StringType(), True),
                StructField("appversion", StringType(), True),
                StructField("event_specific_properties", BinaryType(), True)
            ])
    # Read Parquet file from MinIO
    event_path = f's3a://csb/clickstream_data/{event_name}/{event_date}/events.parquet'
    df = spark.read.schema(schema).parquet(event_path)
    # csb/clickstream_data/consumption_start/2024/1/1/events.parquet
    df = df.withColumn("event_specific_properties",
                   F.from_json(F.expr("CAST(event_specific_properties AS STRING)"),
                               MapType(StringType(), StringType())))
    print(df.schema, end = "\n\n-------------READ PARQUET FILE--------------------------------\n\n")
    # Flatten the schema
    flattened_df = flatten_schema(df)

    print(df.schema, end = "\n\n-------------FLATTENED DF--------------------------------\n\n")

    # Write the flattened DataFrame back to MinIO as Parquet
    delta_table_path = f"s3a://processed/clickstream_data_dlh/{event_name}/delta_table"

    if not DeltaTable.isDeltaTable(spark, delta_table_path):
        # Create the Delta table if it doesn't exist
        flattened_df.write.format("delta").mode("overwrite").save(delta_table_path)
        print(f"Created new Delta table for event: {event_name}")
    else:
        # Append to the existing Delta table
        flattened_df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(delta_table_path)
        print(f"Appended to existing Delta table for event: {event_name}")
    
    

if __name__ == "__main__":
    print("\n\n----------------------MAIN-----------------------\n\n")
    if len(sys.argv) != 3:
        print("Usage: spark-transform.py <event_name> <event_date>")
        sys.exit(1)
    print(sys.argv)

    event_name = sys.argv[1]
    event_date = sys.argv[2].replace("-","/")

    main(event_name,event_date)

    spark.stop()
