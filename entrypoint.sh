#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ]; then
  echo "Starting Spark Master in FOREGROUND..."
  exec spark-class org.apache.spark.deploy.master.Master --host 0.0.0.0 --port 7077 --webui-port 8080
elif [[ "$SPARK_WORKLOAD" == worker* ]]; then
  echo "Starting Spark Worker in FOREGROUND..."
  exec spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
elif [ "$SPARK_WORKLOAD" == "history" ]; then
  echo "Starting Spark History Server..."
  exec spark-class org.apache.spark.deploy.history.HistoryServer
else
  echo "Unknown workload: $SPARK_WORKLOAD"
  exit 1
fi
