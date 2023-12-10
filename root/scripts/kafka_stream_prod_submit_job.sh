#!/bin/bash

# Set Spark home directory
SPARK_HOME=/user/program/spark

# Set application name
APP_NAME=Kafka-Producer

# Set HDFS directory containing the Spark application jar
APP_JAR=/user/program/kafka-producer.jar

# Kafka broker list
KAFKA_BROKERS="localhost:9092"

# Set Kafka topic
KAFKA_TOPIC="events"

# Set HDFS directory for application logs
LOG_DIR=/user/data/logs/kafka-producer

# Build the Spark submit command
spark_submit_cmd="$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --name $APP_NAME \
    --deploy-mode cluster \
    --driver-memory 1g \
    --executor-memory 2g \
    --num-executors 4 \
    --class com.example.kafka-producer \
    $APP_JAR $KAFKA_BROKERS $KAFKA_TOPIC $LOG_DIR"

# Run the Spark application
eval $spark_submit_cmd
