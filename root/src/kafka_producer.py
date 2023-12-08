from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, from_xml
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

spark = SparkSession.builder.appName("KafkaProducer").getOrCreate()

# Below is the schema definition for xml data 
xml_schema = StructType([
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("sever-details", StructType([
        StructField("server_id", IntegerType(), True),
        StructField("location", StringType(), True),
        StructField("temp", FloatType(), True),
    ]), True),
])
 
# Reading XML data with schema
df = (
    spark.readStream
    .format("xml")
    .option("rowTag", "event")
    .schema(xml_schema)
    .load("/Desktop/stream_file.xml")  
)

# Select necessary columns and convert DataFrame to JSON
df_json = df.select(
    to_json(struct("name", "date", "server-details")).alias("value")
)

# Configure Kafka producer parameters
kafka_params = {
    "bootstrap.servers": "localhost:9092",
    "key.serializer": "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer": "org.apache.kafka.common.serialization.StringSerializer"
}

# kafka topic 
kafka_topic = "events"

# Publishing data to Kafka topic in a streaming manner
query = (
    df_json.writeStream
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", kafka_params["bootstrap.servers"])
    .option("topic", "events")
    .start()
)

query.awaitTermination()
