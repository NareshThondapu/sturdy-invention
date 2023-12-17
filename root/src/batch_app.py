from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField

# Create a Spark Session
spark = SparkSession.builder.appName("IncrementalLoadJob").getOrCreate()

# Define HDFS paths for RAW and Processed Zones
hdfs_raw_path = "hdfs://user/data/raw_zone"
hdfs_processed_path = "hdfs://user/data/processed_zone"

# Define schema for the data in RAW Zone
raw_schema = StructType([
    StructField("name", StringType()),
    StructField("date", StringType()),
    StructField("sever-details", StructType([
        StructField("server_id", StringType()),
        StructField("location", StringType()),
        StructField("temp", StringType())
    ]))
])

# Read data from RAW Zone
raw_data = spark.read.schema(raw_schema).parquet(hdfs_raw_path)

# Calculate the previous date
current_date = datetime.now()
current_date_minus_1 = current_date - timedelta(days=1)

# Filter data for the last day
incremental_data = raw_data.filter(unix_timestamp("date") > current_date_minus_1)


processed_data = incremental_data.select(
    col("name"),
    col("date"),
    col("sever-details.server_id").cast("integer").alias("server_id"),
    col("sever-details.location"),
    col("sever-details.temp")
)

# Append processed data in the Processed Zone
processed_data.write.mode("append").partitionBy("date").parquet(hdfs_processed_path)

# Stop the Spark Session
spark.stop()
