import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder.appName("KafkaToHDFS").getOrCreate()

val kafkaTopic = "events"

val kafkaParams = Map(
  "bootstrap.servers" -> "localhost:9092",
  "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
  "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
  "auto.offset.reset" -> "earliest",
  "enable_auto_commit" -> true
)

val hdfsRawPath = "hdfs://user/data/raw_zone"

val eventSchema = StructType(
  Seq(
    StructField("name", StringType, nullable = false),
    StructField("date", StringType, nullable = false),
    StructField("server-details", StructType(
      Seq(
        StructField("server_id", StringType, nullable = false),
        StructField("location", StringType, nullable = false),
        StructField("temp", StringType, nullable = false)
      )
    ))
  )
)

// Read data from Kafka in Structured Streaming
val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", kafkaTopic)
  .load()

// Convert the value column from Kafka message to a string
val jsonValues = df.selectExpr("CAST(value AS STRING)")

// Parse JSON 
val parsedDf = jsonValues
  .select(from_json(col("value"), eventSchema).alias("data"))
  .select(explode(col("data.events")))
  .select(col("col.*"))

//  Data Validation
val validatedDf = parsedDf.filter(
  col("name").isNotNull &&
  col("date").isNotNull &&
  col("server-details.server_id").isNotNull &&
  col("server-details.location").isNotNull &&
  col("server-details.temp").isNotNull &&
  col("server-details.server_id").cast(IntegerType).isNotNull
)

// Data Formatting
val formattedDf = validatedDf.withColumn("name", trim(col("name"))).withColumn("date", trim(col("date")))

// Write the data to HDFS as Parquet in RAW Zone
val query = formattedDf
  .writeStream
  .outputMode("append")
  .format("parquet")
  .option("path", hdfsRawPath)
  .option("checkpointLocation", "hdfs://user/checkpoint/")
  .start()

query.awaitTermination()
