#!/usr/bin/env bash
# Run on the existing Hadoop edge/driver node. Does not change cluster config.
set -euo pipefail
: "${JAVA_HOME:=/usr/local/jdk1.8.0_212}"
: "${HADOOP_CONF_DIR:=/usr/local/hadoop/etc/hadoop}"
: "${SPARK_HOME:=$HOME/opt/retailrocket-runtime/pyspark}"
: "${PYSPARK_PYTHON:=$HOME/opt/cpython-3.11.16-linux-x86_64-gnu/bin/python3.11}"
export JAVA_HOME HADOOP_CONF_DIR SPARK_HOME PYSPARK_PYTHON
jars=$(find "$HOME/opt/retailrocket-jars" -name '*.jar' -printf '%p,' | sed 's/,$//')
exec "$SPARK_HOME/bin/spark-submit" --master yarn --deploy-mode client \
  --driver-memory 512m --executor-memory 512m --executor-cores 1 --num-executors 2 \
  --conf spark.hadoop.yarn.client.failover-sleep-base-ms=500 \
  --conf spark.hadoop.yarn.client.failover-sleep-max-ms=2000 \
  --conf spark.yarn.am.memory=512m --conf spark.executor.memoryOverhead=384 \
  --conf "spark.yarn.am.extraJavaOptions=-XX:CompressedClassSpaceSize=64m -XX:ReservedCodeCacheSize=64m" \
  --conf "spark.executor.extraJavaOptions=-XX:CompressedClassSpaceSize=64m -XX:ReservedCodeCacheSize=64m" \
  --conf spark.dynamicAllocation.enabled=false --conf spark.sql.shuffle.partitions=3 \
  --jars "$jars" "$(dirname "$0")/../retailrocket/spark_batch.py" "$@"
