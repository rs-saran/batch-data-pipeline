from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestSparkJob22-af") \
    .getOrCreate()

df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.show()
