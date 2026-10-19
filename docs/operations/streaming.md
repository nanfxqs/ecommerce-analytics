# V2 有界流处理验证与接入

`retailrocket.spark_stream` 提供 Kafka Structured Streaming → HDFS 原始微批次账本 → 不可变累计 JSON 结果。每个回放轮次使用独立 topic、output、checkpoint；同一组路径同时只允许一个查询。检查点由 Spark 保存，不能删除检查点后继续向同一 output 累计。检查点缺失或损坏明确失败，不自动从头累计。

持续查询先用原生 RocksDB 状态按数据版本、源文件和源行号精确去重，再对内容及有效访客身份维护精确状态；每个微批次只汇总新增身份和全部原始消息，将增量合并到不可变累计快照，不重新扫描整个原始账本。原始消息分支保留所有重试供审计。不同源行的相同内容保留并计入疑似重复，合法乱序不设 watermark 截断。有效/异常判断沿用 V1 的 UTC 时间、访客、商品和事件类型口径。结果包含整个轮次的事件数、跨日独立访客、UTC 分日趋势、异常原因、源记录重试数量、分区独占末端 offset 及最多20条原始样本。

原始数据保存在 `OUTPUT/raw/<microbatch>`，结果保存在 `OUTPUT/snapshots/<microbatch>`。结果先写 `.pending` 再原子重命名；查询只读取有 `_SUCCESS` 且名称为数字的最新目录。处理失败时，Spark 可以重新调用同一个微批次，复用已保存原始数据；已成功写出的结果不重复累计。快照中的 `processed_offsets` 是该持久结果所覆盖的 Kafka 独占末端，不冒充 Kafka 当前末端；积压需要另行读取当前 broker 末端。回调成功后 Spark 才提交检查点，故该两者短暂时间差必须保留语义区分。

后台负责查询存活、停止与“生产已结束且积压清零”的轮次判断。快照 `complete=false` 表示本模块不会自行把流结果宣称为完整历史批次。必须停止并释放流作业，再调用已有历史分析及校验发布入口。

## 运行

沿用 V1 的 edge 隔离 Python/Spark/Kafka connector 和现有集群；不修改 Hadoop、Java、Kafka 配置。先按 V1 运行说明启动集群和 broker，再把本仓库代码与脚本复制到 edge。以下在 edge 执行，专用 topic 已建好，生产者消息身份与 `--run-id` 一致：

```bash
export PYTHONPATH=$HOME/opt/retailrocket-runtime:$HOME/opt/ecommerce-analytics
RETAILROCKET_SHUFFLE_PARTITIONS=2 scripts/submit-stream.sh \
  --brokers hadoop01:9092,hadoop02:9092,hadoop03:9092 \
  --topic MY_RUN --run-id MY_RUN \
  --output hdfs://mycluster/retailrocket/streams/MY_RUN \
  --checkpoint hdfs://mycluster/retailrocket/stream-checkpoints/MY_RUN \
  --max-offsets 10000 --trigger-seconds 2
```

默认持续查询；`--available-now` 消化启动时可见数据后结束，可用相同 checkpoint 再次提交继续。`--evidence PATH` 在成功退出后保存最终快照和 Spark progress。`--starting-offsets` 仅首次启动生效，恢复以检查点为准。`max-offsets` 限制微批次消息量，不能代表生产者速率或业务延迟保证。

可重现已知输入与检查点重启验证（创建新 topic，不覆盖已有数据）：

```bash
RETAILROCKET_SHUFFLE_PARTITIONS=2 \
  $HOME/opt/cpython-3.11.16-linux-x86_64-gnu/bin/python3.11 \
  scripts/verify-stream-feasibility.py --run-id UNIQUE_AUDIT_RUN --work NEW_AUDIT_DIRECTORY
```

第一次发送510条消息，含500个源记录和10次重试。第二次发送501条，包含500个新的更早日期源记录，以及另一源行上的相同内容。最终应有1011次消费、1001个源记录、991有效、10异常、10重试、1疑似内容重复、25个独立访客。

## 性能边界

生产查询使用每作业 RocksDB StateStore（有界原生内存128MiB）维护源身份、内容和有效访客集合，精确状态经 Spark 检查点持久化到 HDFS。仅事件有效的访客进入访客集合；先做源身份去重，避免变化内容的源重试产生额外访客。原始消息分支与身份分支共用输入，Spark progress 的 numInputRows 可能把同一输入计算两次，业务消费量必须取原始消息分支。不能凭该字段宣称完整数据满足10秒页面可见延迟。

旧版累计重扫基线的初始实测 shuffle24、500个源记录的恢复回调为42.229秒，trigger为44.486秒，未达到 V2 延迟目标；随后合并聚合动作并用每作业 shuffle2 复测：预热后99条微批次trigger约5.0–7.1秒，冷启动仍22–33秒。小样本与短时吞吐不代表完整范围满足目标。上述旧版基线配置为2个512MiB heap +384MiB overhead executor。增量流作业保持512MiB heap，并在每作业申请512MiB overhead，2个单核 executor；AM512MiB，JVM class/code cache各64MiB，YARN 内存校验保持启用。完整数据、持续15分钟及浏览器 P95 仍需最终验收；未通过时必须优化或重新与用户确认，不能自行下调门槛。

实测记录见 [流处理证据](../evidence/v2-stream/README.md)。该节点组 UTC 比宿主晚28800秒，未经校准，不能直接跨机器相减估算延迟。最终可由宿主时钟记录 Kafka 发送确认回执到页面观察的时间，或先完成明确的时钟校准。

增量查询的快照格式标记为 `exact-state-v1`。已有累计重扫检查点保留供审计，不尝试静默迁移；升级后应创建新回放轮次。`--observation-file` 可指定 driver 本地小文件，主循环每秒直接核对 HDFS commit 和对应快照后原子更新，无需每次轮询启动 Hadoop JVM。源状态和实体状态必须完整消费，即使同一微批次的外部快照已经写出，恢复时也不能直接跳过整个 DataFrame。

全量压力验证发现，单微批次一次灌入275万条会使512MiB heap无法承受大分区缓存，64MiB RocksDB共享缓存也会发生LRU不足。运行时应使用有界微批次（worker默认1000条），不要将整个数据集视为一批持续处理。增量流默认使用128MiB共享RocksDB缓存及512MiB executor overhead；`RETAILROCKET_STREAM_OVERHEAD_MB` 仅调整该作业内存申请，不修改集群设置。完整回放仍通过多个微批次覆盖全部输入，不能通过丢数据减小状态。
