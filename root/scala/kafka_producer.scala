import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{StreamingContext, _}
import org.apache.spark.streaming.kafka._

val spark = SparkSession.builder.appName("KafkaProducer").getOrCreate()

//  XML schema definition
val xmlSchema = StructType(Seq(
  StructField("name", StringType, nullable = true),
  StructField("date", StringType, nullable = true),
  StructField("sever-details", StructType(Seq(
    StructField("server_id", IntegerType, nullable = true),
    StructField("location", StringType, nullable = true),
    StructField("temp", FloatType, nullable = true)
  )), nullable = true)
))

// Read XML data with schema
val df = spark
  .readStream
  .format("xml")
  .option("rowTag", "event")
  .schema(xmlSchema)
  .load("/Desktop/stream_file.xml")

//convert to JSON
val dfJson = df.select(
  to_json(struct("name", "date", "server-details")).alias("value")
)

val kafkaParams = Map(
  "bootstrap.servers" -> "localhost:9092",
  "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
  "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
)

val kafkaTopic = "events"

// Write data to Kafka topic in streaming
val query = dfJson
  .writeStream
  .format("kafka")
  .outputMode("append")
  .option("kafka.bootstrap.servers", kafkaParams("bootstrap.servers"))
  .option("topic", kafkaTopic)
  .start()

query.awaitTermination()
