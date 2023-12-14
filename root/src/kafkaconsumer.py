from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("KafkaToHDFS").getOrCreate()

# Define Kafka parameters
# Configure Kafka producer parameters
kafka_params = {
    "bootstrap.servers": “localhost:9092",
    "key.serializer": "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer": "org.apache.kafka.common.serialization.StringSerializer",
    "acks":"all"
}

kafka_topic = "events"
# Define HDFS path for RAW Zone
hdfs_raw_path = “hdfs://user/data/raw_zone"

# schema definition 
json_schema = StructType([
    StructField("events", StructType([
        StructField("event", StructType([
            StructField("name", StringType()),
            StructField("date", StringType()),
            StructField("sever-details", StructType([
                StructField("server_id", StringType()),
                StructField("location", StringType()),
                StructField("temp", StringType())
            ]))
        ]))
    ]))
])

# Read data from Kafka in Structured Streaming
df = spark \
    .readStream \
    .format("kafka") \
    .option(“kafka.bootstrap.servers”,”localhost:9092") \
    .option("subscribe", events) \
    .load()

# Convert the value column from Kafka message to a string
json_data = df.selectExpr("CAST(value AS STRING)")

# Parse JSON data using the specified schema
parsed_df = json_data.select(from_json("value", json_schema).alias("data")).select("data.*")

# Apply Data Validation, Dynamic Data Validation, Schema Validation, Data Type Validation
validated_df = parsed_df.filter(
    col("name").isNotNull() &
    col("date").isNotNull() &
    col("sever-details.server_id").isNotNull() &
    col("sever-details.location").isNotNull() &
    col("sever-details.temp").isNotNull() &
    col("sever-details.server_id").cast(IntegerType()).isNotNull()
)

# Apply Data Formatting (Trimming)
formatted_df = validated_df.withColumn("name", trim(col("name"))).withColumn("date", trim(col("date")))


# Write the data to HDFS as Parquet in RAW Zone
query = formatted_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", hdfs_raw_path) \
    .option("checkpointLocation", “hdfs://user/checkpoint/“) \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()
