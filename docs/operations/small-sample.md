# Fixed Retailrocket historical replay

The initial real audit on 2026-10-19 found three existing AlmaLinux8.10 VMs, Hadoop3.1.3 HA, Java8u212, Kafka3.7.0 (Scala2.12), 1536MiB YARN memory per node, and about31GiB free per node. All three VMs were initially stopped. The existing cluster startup script started them; existing configuration and software were preserved. HDFS uses `hdfs://mycluster`. Kafka advertises `hadoop01`, `hadoop02`, `hadoop03`; the edge node and executor nodes resolve these. The host does not resolve them, so replay and submission run on the edge node.

The isolated added runtime is CPython3.11.16 and Spark3.5.6 with matching Scala2.12 Kafka connector3.5.6. The Python dependencies are pinned in `uv.lock`; the five added connector jars have SHA256 checksums in `scripts/connector-sha256.txt`. Existing Java is unchanged. Spark SQL transformations run in the executor JVMs, without requiring executor Python. Compatibility references: [Spark3.5.6](https://spark.apache.org/docs/3.5.6/) and [Kafka integration](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html).

Install from the host with `scripts/install-edge-runtime.sh h1`. This adds directories under `~/opt`; remove only those directories after running jobs to roll back. The script requires `uv`, curl, tar and SSH. The existing cluster must already be running. Copy this repository to `~/opt/ecommerce-analytics` on h1, excluding `.venv`, data and credentials. On h1 set:

```bash
export JAVA_HOME=/usr/local/jdk1.8.0_212
export PATH=$JAVA_HOME/bin:$PATH
export PYTHONPATH=$HOME/opt/retailrocket-runtime:$HOME/opt/ecommerce-analytics
export PYTHON=$HOME/opt/cpython-3.11.16-linux-x86_64-gnu/bin/python3.11
cd ~/opt/ecommerce-analytics
```

Download the official [Retailrocket dataset version2](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset/data). The anonymous download endpoint used successfully was `https://www.kaggle.com/api/v1/datasets/download/retailrocket/ecommerce-dataset?datasetVersionNumber=2`. Use GET (HEAD returned404). Never commit credentials or the downloaded data. Original `events.csv` SHA256 is `3745aa83238b1e6d44d8fda209807899f420084398f94ddf745f3cbcfecbf9e7` and has2,756,101 data rows. `prepare` preserves the full source file as well as the selected rows and records original CSV line numbers (header is line1).

```bash
$PYTHON -m retailrocket.cli prepare --source /path/events.csv --output /path/sample --version kaggle-retailrocket-v2 --limit 10000
~/opt/kafka_2.12-3.7.0/bin/kafka-topics.sh --bootstrap-server hadoop01:9092 --create --topic retailrocket-sample-UNIQUE --partitions 3 --replication-factor 3
$PYTHON -m retailrocket.cli replay --manifest /path/sample/input.json --brokers hadoop01:9092,hadoop02:9092,hadoop03:9092 --topic retailrocket-sample-UNIQUE --batch-id sample-UNIQUE --output /path/sample/replay.json
scripts/submit-batch.sh --replay /path/sample/replay.json --brokers hadoop01:9092,hadoop02:9092,hadoop03:9092 --output hdfs://mycluster/retailrocket/batches/sample-UNIQUE
hdfs dfs -get hdfs://mycluster/retailrocket/batches/sample-UNIQUE /path/export
cp /path/sample/replay.json /path/export/metadata.json
```

Use a dedicated topic and a unique batch/output path. Replay captures start offsets, awaits every producer acknowledgement using bounded asynchronous requests, captures end offsets and checks their difference. Spark reads these exact half-open ranges and verifies count and batch identity before computing. The first10,000 source rows have9,661 views,250 addtocart and89 transaction events. Both the raw Kafka records and Parquet events are persisted in HDFS.

The bounded resource settings request two executors of512MiB heap+384MiB overhead, one core each, and a512MiB AM; YARN rounds allocations to its configured minimum. Per-job JVM class-space and code-cache reservations are capped at64MiB each, preserving the existing YARN virtual-memory enforcement. The first attempt without these caps was killed by that enforcement. This is sequential coursework infrastructure, not a production sizing claim. Final evidence must include different executor hosts, successful YARN application, HDFS files and DuckDB result counts; local tests alone do not satisfy distributed acceptance.

After export, on the host use `uv run streamlit run retailrocket/dashboard.py -- /path/export` for the initial overview. Later tickets add validated publication and the complete dashboard. This baseline explicitly labels historical replay, batch, input event date range and UTC.
