import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


val spark = SparkSession.builder.appName("batch-app").getOrCreate()

// Define HDFS paths for RAW and Processed Zones
val hdfsRawPath = "hdfs://user/data/raw_zone"
val hdfsProcessedPath = "hdfs://user/data/processed_zone"

// Define schema for the data in RAW Zone
val rawSchema = StructType(Seq(
  StructField("name", StringType),
  StructField("date", StringType),
  StructField("sever-details", StructType(Seq(
    StructField("server_id", StringType),
    StructField("location", StringType),
    StructField("temp", StringType)
  )))
))

// Read data from RAW Zone
val rawData = spark.read.schema(rawSchema).parquet(hdfsRawPath)

// Calculate the timestamp for the current hour - 1
val currentDateTimeMinus1 = current_timestamp() - hours(1)

// Filter data for the last day
val incrementalData = rawData.filter(unix_timestamp(col("date")) > currentDateTimeMinus1)

val processedData = incrementalData.select(
  col("name"),
  col("date"),
  col("sever-details.server_id").cast(IntegerType).alias("server_id"),
  col("sever-details.location"),
  col("sever-details.temp")
)

// Append processed data in the Processed Zone
processedData.write.mode("append").partitionBy("date").parquet(hdfsProcessedPath)

spark.stop()
