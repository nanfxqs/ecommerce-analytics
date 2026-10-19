# Retailrocket 历史行为分析

使用现有三节点 Kafka、HDFS HA 和 YARN，回放 Retailrocket 历史事件；Spark 在集群清洗、关联历史分类并计算结果，本机 DuckDB 查询不可变 Parquet，Streamlit + Plotly 展示概览、购买关联、商品与分类、用户画像及质量摘要。所有日期使用源事件 UTC，回放不表示实时业务操作。

## 环境与数据

已验证环境：Hadoop3.1.3、Java8u212、Kafka3.7.0，新增隔离的 CPython3.11.16、Spark3.5.6/Scala2.12 及匹配 Kafka 连接器；本机依赖锁定在 `uv.lock`。运行记录见 [验收证据](docs/evidence/)。三个虚拟节点共享一个物理宿主，不提供宿主故障容灾。虚拟机初始状态与网络、资源、时钟约束见 [运行说明](docs/operations/small-sample.md)。

宿主机先启动现有集群和三个 Kafka broker，确认 SSH `h1` 可用；不要替换已有 Hadoop/Java/Kafka 配置。需要 `uv`、curl、tar、SSH。安装只在 edge 节点 `~/opt` 添加独立运行时：

```bash
# 本机已有相邻 hadoop-cluster 仓库的情况下，复用原启停脚本
bash ../hadoop-cluster/start-cluster.sh
for edge in h1 h2 h3; do
  ssh "$edge" 'export JAVA_HOME=/usr/local/jdk1.8.0_212; "$JAVA_HOME/bin/jps" | grep -q Kafka || ~/opt/kafka_2.12-3.7.0/bin/kafka-server-start.sh -daemon ~/opt/kafka_2.12-3.7.0/config/server.properties'
done
uv sync --python 3.11.16 --extra spark --extra dev --frozen
scripts/install-edge-runtime.sh h1
ssh h1 'mkdir -p ~/opt/ecommerce-analytics ~/opt/retailrocket-source-v2'
scp -r retailrocket scripts h1:~/opt/ecommerce-analytics/
```

唯一首版数据源为 [官方 Retailrocket 数据集 version2](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset/data)。实施时公开 GET 无需认证；HEAD 返回404不能证明下载不可用。如果平台以后要求认证，只在本机配置，不提交凭据。

```bash
mkdir -p data/retailrocket-v2
curl --fail --location 'https://www.kaggle.com/api/v1/datasets/download/retailrocket/ecommerce-dataset?datasetVersionNumber=2' -o data/retailrocket-v2.zip
unzip data/retailrocket-v2.zip -d data/retailrocket-v2
sha256sum data/retailrocket-v2/*.csv
scp data/retailrocket-v2/*.csv h1:~/opt/retailrocket-source-v2/
```

`events.csv` 应有2,756,101条数据，SHA256为 `3745aa83238b1e6d44d8fda209807899f420084398f94ddf745f3cbcfecbf9e7`。两份商品属性共20,275,902行，分类树1,669行。准备阶段记录实际 SHA256、版本和过滤前源行号，原始文件保留。不要提交数据目录。

## 在 edge 节点准备、回放与计算

以下命令在 `ssh h1` 后运行。每次新的完整快照使用新的批次 ID/专用 topic/HDFS 路径；单任务顺序执行。小样本仅在 `prepare` 增加 `--limit 10000`，仍然走相同 Kafka/YARN 链路。

```bash
export JAVA_HOME=/usr/local/jdk1.8.0_212
export PATH=$JAVA_HOME/bin:/usr/local/hadoop/bin:$PATH
export PYTHONPATH=$HOME/opt/retailrocket-runtime:$HOME/opt/ecommerce-analytics
export PYTHON=$HOME/opt/cpython-3.11.16-linux-x86_64-gnu/bin/python3.11
export BATCH=demo-v2-001
export WORK=$HOME/opt/retailrocket-runs/$BATCH
mkdir -p "$WORK"
cd ~/opt/ecommerce-analytics

hdfs dfsadmin -report
yarn node -list
~/opt/kafka_2.12-3.7.0/bin/kafka-broker-api-versions.sh --bootstrap-server hadoop01:9092

$PYTHON -m retailrocket.cli prepare --source ~/opt/retailrocket-source-v2/events.csv --output "$WORK/input" --version kaggle-retailrocket-v2 --store "$WORK/state" --batch-id "$BATCH"
hdfs dfs -mkdir -p hdfs://mycluster/retailrocket/references
$PYTHON -m retailrocket.cli prepare-references --properties ~/opt/retailrocket-source-v2/item_properties_part1.csv ~/opt/retailrocket-source-v2/item_properties_part2.csv --tree ~/opt/retailrocket-source-v2/category_tree.csv --output "$WORK/references" --version kaggle-retailrocket-v2 --events-manifest "$WORK/input/input.json" --hdfs-directory "hdfs://mycluster/retailrocket/references/$BATCH"
~/opt/kafka_2.12-3.7.0/bin/kafka-topics.sh --bootstrap-server hadoop01:9092 --create --topic "$BATCH" --partitions 3 --replication-factor 3
$PYTHON -m retailrocket.cli replay --manifest "$WORK/input/input.json" --brokers hadoop01:9092,hadoop02:9092,hadoop03:9092 --topic "$BATCH" --batch-id "$BATCH" --output "$WORK/replay.json" --store "$WORK/state"
scripts/submit-batch.sh --replay "$WORK/replay.json" --brokers hadoop01:9092,hadoop02:9092,hadoop03:9092 --output "hdfs://mycluster/retailrocket/batches/$BATCH" --references "hdfs://mycluster/retailrocket/references/$BATCH" --store "$WORK/state" > "$WORK/spark.log" 2>&1
scripts/export-batch.sh "hdfs://mycluster/retailrocket/batches/$BATCH" "$WORK/export" "$WORK/spark-evidence.json"
```

回放确认全部发送完成才写 `replay.json`，分区区间是 `[startingOffset, endingOffset)`。其他生产者不得同时写入专用 topic。Spark 核对 batch、消费量、源行完整性，按源身份去掉采集重试；不同源行的相同事件保留并计入疑似重复。商品属性直接进入 HDFS，不经过 Kafka。分类不使用未来快照。

## 在宿主机发布并启动看板

```bash
mkdir -p runs/demo-v2-001
scp -r h1:~/opt/retailrocket-runs/demo-v2-001/export runs/demo-v2-001/
scp h1:~/opt/retailrocket-runs/demo-v2-001/replay.json runs/demo-v2-001/
uv run retailrocket publish --store runs/published --replay runs/demo-v2-001/replay.json --export runs/demo-v2-001/export
uv run retailrocket status --store runs/published
uv run streamlit run retailrocket/dashboard.py -- runs/published
```

展示时先说明历史回放、当前批次、源数据范围和 UTC。日期选择同步作用于所有页面。购买关联使用范围独立访客交集及明确分母，不要求先后顺序；跨日访客不会相加。画像支持精确访客 ID、分页，最近活跃间隔以所选范围内最大事件时间为基准，分类偏好按浏览排序，购买单独展示。空分母显示无数据。

发布器验证全部结果表、源/参考文件 provenance 和数量关系后复制不可变快照并原子切换。失败保持此前成功快照；看板一次计算绑定同一成功快照。不要手工修改 `published/batches`。

## 重算、失败与验证

HDFS raw 和原始输入持久保存，Kafka 不承担永久保留责任。相同批次重算使用上方 Spark 命令增加 `--retry-from-raw`；缺失 raw 会明确失败，不回退到 Kafka。结果覆盖该批次工作目录，不累加指标。再次导出时必须换一个新的本机目标目录，然后重新执行 publish。原成功发布目录保持不变。

```bash
# 在 edge 节点，复用相同 replay/output/references/store 参数
scripts/submit-batch.sh --replay "$WORK/replay.json" --brokers hadoop01:9092,hadoop02:9092,hadoop03:9092 --output "hdfs://mycluster/retailrocket/batches/$BATCH" --references "hdfs://mycluster/retailrocket/references/$BATCH" --store "$WORK/state" --retry-from-raw > "$WORK/retry.log" 2>&1
scripts/export-batch.sh "hdfs://mycluster/retailrocket/batches/$BATCH" "$WORK/export-retry" "$WORK/spark-evidence.json"
```

可控失败演练、已发布结果保护及重试证据见最终验收记录。准备失败后仅清理该次未完成的准备目录；回放失败使用重新确认的 manifest，不发布部分回放。保留 input/replay、参考 manifest、状态、Spark/YARN 日志与 HDFS raw。

测试统一通过公开批次与查询边界；本地测试只证明语义，不替代真实分布式验收：

```bash
JAVA_HOME=/path/to/jdk8 PYSPARK_PYTHON=$PWD/.venv/bin/python uv run --extra spark --extra dev pytest -q
```

固定全量输入的事件数应为：浏览2,664,312，加购69,332，购买22,457。分类覆盖率、异常与疑似重复数以实际批次质量输出为准。资源参数为两个512MiB heap+384MiB overhead executor、一个512MiB AM，每个1core，默认24个 shuffle 分区；`RETAILROCKET_SHUFFLE_PARTITIONS` 可按实测调整。保留既有 YARN 内存校验，JVM class-space/code-cache 分别限制64MiB。不要宣称无依据的性能保证。
