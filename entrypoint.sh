#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ]; then
  echo "Starting Spark Master..."
  start-master.sh -p 7077
elif [[ "$SPARK_WORKLOAD" == worker* ]]; then
  # Start worker with the appropriate worker name (worker-1, worker-2, worker-3)
  echo "Starting Spark Worker: $SPARK_WORKLOAD"
  start-worker.sh spark://spark-master:7077
elif [ "$SPARK_WORKLOAD" == "history" ]; then
  echo "Starting Spark History Server..."
  start-history-server.sh
else
  echo "Unknown workload: $SPARK_WORKLOAD"
  exit 1
fi
