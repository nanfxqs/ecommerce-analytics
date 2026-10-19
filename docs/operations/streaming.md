# V2 有界流处理验证与接入

`retailrocket.spark_stream` 提供 Kafka Structured Streaming → HDFS 原始微批次账本 → 不可变累计 JSON 结果。每个回放轮次使用独立 topic、output、checkpoint；同一组路径同时只允许一个查询。检查点由 Spark 保存，不能删除检查点后继续向同一 output 累计。检查点缺失或损坏明确失败，不自动从头累计。

每个成功微批次先保存 Kafka 原始记录，再按数据版本、源文件和源行号精确去重。不同源行的相同内容保留并计入疑似重复，合法乱序不设 watermark 截断。有效/异常判断沿用 V1 的 UTC 时间、访客、商品和事件类型口径。结果包含整个轮次的事件数、跨日独立访客、UTC 分日趋势、异常原因、源记录重试数量、分区独占末端 offset 及最多20条原始样本。

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

当前方案每次从 HDFS 原始账本重算累计聚合，优先保证有界内存、可审计输入和精确恢复，代价是累计输入越多扫描成本越高。检查点不在 executor 内存中保存无限增长的访客集合。该实现是可运行的正确性基线，不能凭小样本或 Spark 的 `processedRowsPerSecond` 宣称完整数据满足10秒页面可见延迟。

初始实测 shuffle24、500个源记录的恢复回调为42.229秒，trigger为44.486秒，未达到 V2 延迟目标；随后合并聚合动作并用每作业 shuffle2 复测：预热后99条微批次trigger约5.0–7.1秒，冷启动仍22–33秒。小样本与短时吞吐不代表完整范围满足目标。既有资源配置仍为2个512MiB heap +384MiB overhead executor，单核，AM512MiB，JVM class/code cache各64MiB。完整数据、持续15分钟及浏览器 P95 仍需最终验收；未通过时必须优化或重新与用户确认，不能自行下调门槛。

实测记录见 [流处理证据](../evidence/v2-stream/README.md)。该节点组 UTC 比宿主晚28800秒，未经校准，不能直接跨机器相减估算延迟。最终可由宿主时钟记录 Kafka 发送确认回执到页面观察的时间，或先完成明确的时钟校准。
