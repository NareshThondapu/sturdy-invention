#!/bin/bash

# Set Spark home directory
SPARK_HOME=/path/to/spark

# Set application name
APP_NAME=Kafka-consumer

# Set HDFS directory containing the Spark application jar
APP_JAR=/user/program/my-streaming-app.jar

# Set Kafka broker list
KAFKA_BROKERS="localhost:9092"

# Set Kafka topic
KAFKA_TOPIC="events"

# Set HDFS directory for RAW Zone
HDFS_RAW_PATH="hdfs://user/data/raw_zone"

# Set checkpoint directory for streaming application
CHECKPOINT_DIR="hdfs://user/data/checkpoint/"

# Build the Spark submit command
spark_submit_cmd="$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --name $APP_NAME \
    --deploy-mode cluster \
    --driver-memory 1g \
    --executor-memory 2g \
    --num-executors 4 \
    --class com.example.kafka-consumer \
    $APP_JAR $KAFKA_BROKERS $KAFKA_TOPIC $HDFS_RAW_PATH $CHECKPOINT_DIR"

# Run the Spark application
eval $spark_submit_cmd
