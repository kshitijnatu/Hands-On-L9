from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalyticsTask2").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

# Read streaming data from socket
raw_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Parse JSON data into columns using the defined schema
parsed_df = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Compute aggregations: total fare and average distance grouped by driver_id
aggregated_df = parsed_df.groupBy("driver_id").agg(
    sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Write each micro-batch to CSV (one folder per batch)
def write_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    output_path = f"outputs/task_2/batch_{batch_id}"
    (
        batch_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )

# CSV output in outputs/task_2
csv_query = (
    aggregated_df.writeStream
    .outputMode("update")
    .foreachBatch(write_batch)
    .option("checkpointLocation", "outputs/checkpoints/task_2_csv")
    .start()
)

# Console output (README asks for real-time console view)
console_query = (
    aggregated_df.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .start()
)

spark.streams.awaitAnyTermination()
