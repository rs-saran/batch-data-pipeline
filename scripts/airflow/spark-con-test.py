from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TestSparkJob").getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    df.show()

    spark.stop()
