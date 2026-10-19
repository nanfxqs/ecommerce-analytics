# V2 流处理前置验证（2026-10-19）

本记录只证明已知样本真实经过 Kafka → YARN Structured Streaming → HDFS 持久结果及检查点恢复，不代表 V2 全量、页面或性能验收通过。运行代码在 `v2/t10-stream`，基线 b7ab20d，恢复与聚合优化0e19471；后续本目录证据提交记录精确版本。

复用原有三台虚拟机、Kafka、HDFS HA 和 YARN，启动脚本及既有配置未改动。宿主通过 SSH h1/h2/h3 访问，Kafka 的 hadoop01–03 地址只在集群内解析。隔离 Python3.11.16、Spark3.5.6、Scala2.12 connector及原Java8不变。

专用 topic `v2-stream-feasibility-20261019`，三分区、三副本；轮次 `v2-feasibility`。HDFS 结果 `/retailrocket/streams/v2-feasibility`，检查点 `/retailrocket/stream-checkpoints/v2-feasibility`。edge 代码与原始日志保留在 `~/opt/ecommerce-v2-stream`，不改 V1 正式批次、topic和发布存储。

## 正确性与真实失败恢复

1. 发送510条：500个源记录和10次源记录重试。首次 application_1792382297034_0001 已保存 raw，写结果时因快照写入误触发 executor Python 而失败，未提交流检查点。修复写入为 JVM SQL 后，以**相同 HDFS 检查点与 output**重启。
2. application_1792382297034_0002 成功，参见 [first.json](first.json)：消费510、源记录500、有效500、异常0、重试10、独立访客25。该次读取此前保存的原始微批次，Spark progress 的 numInputRows=0，不能据此声称没消费；累计消费量和独占末端以结果账本为准。
3. 再发送501条：500个新源记录（更早日期，合法乱序），其中10个无效时间戳；额外1条为不同源行上与已有记录相同的内容。相同检查点恢复 application_1792382297034_0003，参见 [resumed.json](resumed.json)：消费1011、源记录1001、有效991、异常10、重试10、疑似内容重复1、独立访客25，三个分区处理独占末端均337。
4. 两次成功快照的 executor_hosts 均包含 hadoop02 和 hadoop03，driver 在 hadoop01；实际任务日志也记录两个 executor 节点。不是 local 模式。

本地公开 pipeline 集成测试通过（35.25秒），另以**同一捕获输入**调用既有 V1 `analyze`，核对源记录、有效/异常、重试、疑似内容重复及跨日独立访客，固定期望值独立写定。该测试仅验证语义，不代替上述 YARN 证据。

## 资源与延迟限制

- 使用两个 executor，各512MiB heap+384MiB overhead、1核；AM512MiB。JVM class-space/code-cache各64MiB，保留既有 YARN 内存检查。
- 初始 shuffle24 恢复 callback addBatch42.229秒，trigger44.486秒。
- 合并聚合动作并设每作业 shuffle2 后，501条新增消息的冷启动 callback32.258秒、trigger32.974秒，reported15.193条/秒。该值是一次 burst 的处理吞吐，**不是经15分钟验证的持续输入速率**。
- [warm.json](warm.json) 记录再次重放510条已存在源记录，maxOffsetsPerTrigger=100，分六个微批次处理（99/99/99/99/99/15条）。首次trigger22.303秒；随后为7.085、5.461、5.008、5.356、4.791秒。最终消费1521、源记录仍1001、重试520，其余业务指标不变。预热后的99条微批次约13.97–19.77条/秒，这仍是短时处理吞吐，不是稳定输入验收。完整输入下的持续速率、状态增长、15分钟运行、页面可见延迟 P95≤10秒均尚未通过。
- [HDFS 状态实测](hdfs-state.txt)：本次8个原始微批次及快照共563731逻辑字节，检查点6471逻辑字节，commit0–7全部存在；这是1001个源记录的样本规模，不能外推全量状态成本。
- warm 作业初期一次进程采样：hadoop02 executor pid3892 RSS298536KiB，hadoop03 executor pid4055 RSS427460KiB；这是采样值，不是峰值，更不能推断全量资源上限。
- 同时观测 guest epoch1792382493、host1792411293，节点 UTC 比宿主晚28800秒。本验证没有校时，不能用跨机器 wall-clock 差声明端到端延迟。最终验收需同一宿主时钟量测发送确认回执与页面首次可见，或明确校准后再量测。

完整运行接口、脚本和性能边界见 [流处理运行说明](../../operations/streaming.md)。目前累计账本重扫是正确性基线；后续必须用实测推进优化，不能降低父规格的性能目标或把本证据标成 V2 验收完成。
