#!/bin/bash

# Set Spark home directory
SPARK_HOME=/user/programs/spark

# Set application name
APP_NAME=batch-app

# Set HDFS directory containing the Spark application jar
APP_JAR=/user/programs/batch-app.jar

# Set HDFS directory for application logs
LOG_DIR=/user/data/logs/batch-app

# Define date for today
CURRENT_DATETIME=$(date "+%Y-%m-%d %H:%M:%S")

# Build the Spark submit command
spark_submit_cmd="$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --name $APP_NAME \
    --deploy-mode cluster \
    --driver-memory 1g \
    --executor-memory 2g \
    --num-executors 4 \
    --files /path/to/configuration.yaml \
    --class com.example.batch-app \
    $APP_JAR $CURRENT_DATETIME $LOG_DIR"

# Run the Spark application
eval $spark_submit_cmd
