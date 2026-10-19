# V2 最终验收运行记录

2026-10-19：实现已进入最终验收，尚不标记 V2 完成。所有实际结果须同时满足全量正确性、真实分布式链路、至少 15 分钟正常负载以及端到端 P95≤10 秒；最终审核也须通过。

## 资源与部署配置

复用既有 Hadoop 3.1.3、Kafka 3.7.0、Java 8u212，隔离 CPython 3.11.16、Spark 3.5.6 / Scala 2.12。流任务两个单核 executor，各 512MiB heap +512MiB overhead，AM512MiB，RocksDB 共享有界原生缓存128MiB；JVM class-space/code-cache 各64MiB。保持 YARN 内存校验，不修改全局配置。历史任务采用既有批处理脚本资源。机器为同一宿主上的三个节点，跨节点成功不能证明宿主故障容灾。

worker operator JSON 支持：

| 配置 | 默认 | 最终验收部署 |
| --- | --- | --- |
| `max_offsets` | 1000 | 全量100000；正常小样本1000 |
| `shuffle_partitions` | full24、small2 | 全量24；正常小样本2 |
| `cursor_flush_records` | 1 | 100 |
| `cursor_flush_seconds` | 1 | 1 |
| `wait_for_stream_ready` | false | 正常延迟验收true |
| `audit_ack_log` | false | 正常延迟验收true |
| `observation_interval_seconds` | 1 | 正常延迟验收0.1 |

`stream_profile` 固定轮次状态分区数；恢复拒绝改变分区配置，旧版未记录 profile 的检查点按原2分区处理。更改分区数创建新轮次，不迁移或覆盖旧检查点。游标批量落盘仅影响故障重发上界，源身份精确去重保证结果；暂停、停止、EOF 强制落盘。正常验收的 readiness 在专属空 topic 上等待 executor 注册与空查询预热后才允许生产，第一条 ACK 也纳入统计，没有隐藏数据预热或启动样本排除。

## 可复现验收入口

先按 [回放说明](replay.md) 和 [自动发布说明](automatic-publication.md) 注册已核验的固定小样本10000条、完整数据2756101条以及参考文件，使用新的专属 run root / 发布 store。API 在本机回环地址运行，前端每2秒轮询。

```bash
# 完整真实链路；也可 --attach-run 已启动轮次
python scripts/acceptance-pipeline.py --api http://127.0.0.1:8006 --out NEW_FULL_EVIDENCE
# 完整结果已成功发布后，实际 Chromium 检查历史五类页面与实时结果
(cd frontend && V2_FULL_ACCEPTANCE=1 npx playwright test --config playwright.acceptance.config.ts)
# 正常 API8007、前端5179；务必独占集群并停止宿主重测试等干扰负载
node scripts/acceptance-performance.mjs \
  --cluster LOCAL_SSH_CLUSTER_JSON --edge LOCAL_EDGE_CONFIG_JSON \
  --api http://127.0.0.1:8007 --frontend http://127.0.0.1:5179 \
  --seconds 900 --rate 5 --out NEW_PERFORMANCE_EVIDENCE
# 实际游标 fsync 开销；只在自己的临时目录写入
python scripts/benchmark-cursor.py --help
```

正常验收 API 使用 `--poll-seconds 0.25`，后台 SSH 观察串行执行，调用耗时与配置等待共同决定实际缓存周期；前端轮询仍为2秒。驱动每0.1秒检查新 checkpoint commit，只在新不可变提交出现时完整读取并验证 snapshot/offset；恢复后的首次读取始终重新验证。原始样本独立读取 Kafka，业务快照保留旧 `samples` / `sample_records` 字段为空列表，消除一次多余的分布式样本排序收集，全部原始 Kafka 消息仍持久落盘。

性能脚本读取真实 Kafka `Future.add_callback` 的 partition/offset 与 ACK 单调时间，而非稍后更新的连续游标计数。Chromium DOM 在两次 animation frame 后报告实际绘制的同轮次、同 topic、同 partition 的 exclusive committed offset，逐条匹配首次覆盖该 ACK 的页面；单靠事件总数不构成可见性证据。脚本通过 SSH 往返包围的 guest monotonic / host hrtime 校准，在开始、每分钟和结束记录偏移与不确定度；P95 使用包含校准误差的保守上界。全部 ACK 样本纳入，故意制造积压、恢复和最终停止排空阶段分别记录。输出包含原始 ACK、绘制帧、逐条延迟、校准候选、浏览器轮询、每秒后端观察和每分钟积压分布。

当前节点 UTC 比宿主晚28800秒，禁止未经校准直接相减。浏览器绑定回传延迟保守计入，不扣除。验收脚本要求实际 ACK 跨度≥900秒、全部 ACK 有页面覆盖、P95上界≤10000ms、无持续增长的积压，提前停止不改变已发布完整历史。

## 当前真实结果与失败保留

- 已有流可行性、控制、恢复及自动发布证据分别位于 `docs/evidence/v2-stream`、`docs/evidence/v2-controls`、`docs/evidence/v2-recovery`、`docs/evidence/v2-publication`。已知输入验证源重试、内容相同的不同源行、异常、乱序和跨日访客；恢复包含 ACK35 / durable cursor34、重发不翻倍及损坏检查点拒绝。它们不能替代本次完整链路和15分钟验收。
- 旧版每批累计重扫全量 trigger126秒，已被增量状态架构替代。独立旧topic全量探测的24分区 / 100k微批次结果与 V1 完全一致；这是状态规模证据，不是完整 V2 生产者链路验收。
- 本次实际完整 worker 轮次 `v2-b68af3f678234c1aae86dd6e851fc54e` 使用2分区，在 confirmed1516976时因 RocksDB `Insert failed due to LRU cache being full` 失败。保留全部 topic、检查点及日志，不发布部分结果。该失败说明小样本2分区资源配置不能直接用于全量。
- 随后以新隔离轮次 `v2-58e8ae0622ec45269dd5e147cb226a3f` 使用24分区重做完整链路。该轮次真实全链路已成功，流应用0018和历史应用0019均 FINISHED/SUCCEEDED；新发布历史与实时417个 UTC 行为桶逐一一致。实际 Chromium 完整页面1项测试通过7.7秒，详见 [永久证据](../evidence/v2-acceptance/README.md)。完整注册数据与 V1 来源校验一致，原始 V1 结果和topic均未修改。
- 实际边缘 fsync1000次在并行全量负载下：每条落盘11.289秒，100条批量落盘0.102秒。实际完整生产者约2500–2700条/秒；请求速率100000仅为上限，不是已实现吞吐或10秒延迟承诺。全量突发积压与另行5条/秒正常延迟验收分别报告。
- 容量观察：edge根盘36GiB中15GiB空闲，HDFS105.1GiB中55.0GiB可用、7.7GiB使用；不执行自动清理。

全量与完整浏览器核对已通过；15分钟正常输入与最终审核仍待完成。当前不标记 V2 完成。


120秒最终观察路径预检的607条普通窗口 ACK 全部匹配首次页面绘制，但P95保守上界13621.7ms，未达到10秒；原始证据保留，未直接开始900秒。故意关闭浏览器15秒测试独立通过，后台源游标629→712、提交量610→682，重新打开同轮次后结果可见，提前停止后751条消息排空且旧完整历史保持不变。后续优化使用上述0.1秒驱动观察与0.25秒后台等待，并删除旧 Spark 样本收集动作；新的真实预检及900秒正式结果待完成，不下调目标。
