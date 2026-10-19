# Retailrocket 历史行为分析

使用现有三节点 Kafka、HDFS HA 和 YARN，回放 Retailrocket 历史事件；Spark 在集群清洗并持续处理，完整轮次再关联历史分类计算结果；本机 DuckDB 查询不可变 Parquet，React + FastAPI 提供实时监控、回放控制与历史分析，保留 V1 Streamlit + Plotly 看板。所有日期使用源事件 UTC，回放不表示实时业务操作。

## 版本与路线图

- **V1：已完成并验收。** 当前代码与下方运行命令对应历史数据批处理、Streamlit + Plotly 看板和可靠发布；[全量验收证据](docs/evidence/full/README.md)。
- **V2：已实现，最终验收中。** React + TypeScript + Vite 页面、FastAPI、Structured Streaming、回放控制、恢复与自动历史发布已实现；真实全量链路及完整页面已通过，连续15分钟正常负载 P95≤10秒及最终审核仍待完成。[验收记录](docs/operations/v2-acceptance.md)。

完整版本范围和计划见 [项目路线图](roadmap.md)（[详细路线图](docs/roadmap.md)）。这里的 V1 / V2 是项目里程碑；数据集 `version2` 和现有名称中的 `v2` 是数据版本标识，两者独立。

## 环境与数据

已验证环境：Hadoop3.1.3、Java8u212、Kafka3.7.0，新增隔离的 CPython3.11.16、Spark3.5.6/Scala2.12 及匹配 Kafka 连接器；本机依赖锁定在 `uv.lock`。运行记录见 [全量验收证据](docs/evidence/full/README.md)。三个虚拟节点共享一个物理宿主，不提供宿主故障容灾。虚拟机初始状态与网络、资源、时钟约束见 [运行说明](docs/operations/small-sample.md)。

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

V1 唯一数据源为 [官方 Retailrocket 数据集 version2](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset/data)。实施时公开 GET 无需认证；HEAD 返回404不能证明下载不可用。如果平台以后要求认证，只在本机配置，不提交凭据。

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

在宿主机可用下列命令演练发布前失败：脚本复制有效导出并添加一个明确的验收 marker，创建不同 revision；注入失败后核对旧指针和查询数量，再重试并核对指针实际切换且指标不累加。候选目录必须不存在，分析数据不会修改。

```bash
uv run python scripts/verify-publication-recovery.py runs/published runs/demo-v2-001/replay.json runs/demo-v2-001/export runs/demo-v2-001/export-fault-candidate > runs/demo-v2-001/recovery-report.json
```

实际可控失败、已发布结果保护及重试证据见最终验收记录。准备失败后仅清理该次未完成的准备目录；回放失败使用重新确认的 manifest，不发布部分回放。保留 input/replay、参考 manifest、状态、Spark/YARN 日志与 HDFS raw。

测试统一通过公开批次与查询边界；本地测试只证明语义，不替代真实分布式验收：

```bash
JAVA_HOME=/path/to/jdk8 PYSPARK_PYTHON=$PWD/.venv/bin/python uv run --extra spark --extra dev pytest -q
```

固定全量输入的事件数应为：浏览2,664,312，加购69,332，购买22,457。分类覆盖率、异常与疑似重复数以实际批次质量输出为准。资源参数为两个512MiB heap+384MiB overhead executor、一个512MiB AM，每个1core，默认24个 shuffle 分区；`RETAILROCKET_SHUFFLE_PARTITIONS` 可按实测调整。保留既有 YARN 内存校验，JVM class-space/code-cache 分别限制64MiB。不要宣称无依据的性能保证。

## V2 本机 React 历史概览

V2 页面查询既有成功发布的不可变分析批次；刷新页面不会启动 Spark。两个服务均绑定 `127.0.0.1`。保持下列两个终端运行：

```bash
uv sync --extra dev
uv run python -m retailrocket.api --store /home/nanfl/.cache/retailrocket-published
```

```bash
cd frontend
npm ci
npm run dev
```

浏览器打开 `http://127.0.0.1:5173`。日期为 UTC，包含结束日全天；页面每 2 秒查询。`npm run build` 检查 TypeScript 并生成静态产物，`npm run preview` 可在本机预览构建（API 仍须运行）。

HTTP 与浏览器测试复用经过完整 Spark 处理及发布校验的已知输入，不能用这些本地测试替代 YARN 分布式验收：

```bash
JAVA_HOME=/path/to/jdk8 PYSPARK_PYTHON=$PWD/.venv/bin/python uv run --extra spark --extra dev pytest tests/test_history_api.py -q
JAVA_HOME=/path/to/jdk8 PYSPARK_PYTHON=$PWD/.venv/bin/python uv run --extra spark python tests/history_fixture.py .scratch/browser-history
npm --prefix frontend run test:e2e
```

浏览器测试使用 `/usr/bin/chromium`，也可通过 `CHROMIUM_PATH` 指定现有 Chromium。测试占用本机 8000 和 5173 端口，运行前停止相同端口的开发服务。再次生成夹具时使用新的目录或保留原有夹具直接运行浏览器测试。

V2 历史分析导航还提供购买关联、分类偏好、用户画像及数据质量，所有页面共用 UTC 日期范围。画像支持访客 ID 精确查询、每页数量、偏好分类数和页码跳转；分类保留 `unknown`，购买比率分母为零时显示“无数据”。质量页区分日期范围统计和整个批次统计，默认展示整个批次的异常，因此无效时间戳仍可追溯。切换到日期范围异常时，仅包含能归属所选 UTC 日期的记录。

V2 产物默认永久保留。需要释放空间时，使用[离线手动清理命令](docs/operations/cleanup.md)：先停止 API 并等待集群空闲，默认预览，显式 `--apply` 才删除已结束轮次。当前发布批次、活动轮次及未知路径受保护。

正常完整回放自动顺序执行 YARN 历史分析、导出传回和完整校验发布。需要在 edge 注册参考文件校验和；配置、阶段与失败重试见[自动历史发布说明](docs/operations/automatic-publication.md)。提前停止不进入历史发布，失败继续展示此前成功批次。


V2 实时回放、控制、手动恢复与最终验收分别见 [回放说明](docs/operations/replay.md)、[流处理说明](docs/operations/streaming.md) 和 [验收运行记录](docs/operations/v2-acceptance.md)。流任务使用两个 512MiB heap + 512MiB overhead executor、128MiB RocksDB 共享原生缓存及 512MiB AM；历史任务仍采用原批处理资源。分区数影响完整精确状态的资源压力：小样本默认 2，全量部署使用已验证的 24；检查点固定状态分区数，调整须使用新隔离轮次。未完成最终全量与 15 分钟浏览器性能验收前，不声称 V2 已完成或保证实测之外的速率。


V2 正常负载验收使用明确的部署配置：API 后台串行缓存等待0.25秒，驱动观察0.1秒，浏览器2秒。带集群控制的启动入口为：

```bash
uv run python -m retailrocket.api --store /operator/published \
  --cluster-config /operator/local-cluster.json --poll-seconds 0.25
```

edge 的正常输入 profile 设置 `observation_interval_seconds: 0.1`、`wait_for_stream_ready: true`、`cursor_flush_records: 100`、`cursor_flush_seconds: 1`、`rocksdb_changelog: true`，小样本默认2个状态分区。全量突发 profile 使用 `max_offsets: 100000`、默认24个状态分区及本次已验收的 `rocksdb_changelog: false`；切换操作者配置前必须确认没有活动轮次，恢复时保持原状态分区数。完整配置、普通负载速率及验收结果见 [V2 运行记录](docs/operations/v2-acceptance.md)。上述较低后台等待不改变页面每2秒轮询的行为，也不并发启动无界 SSH 查询。
