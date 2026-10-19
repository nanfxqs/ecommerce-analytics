# V2 最终验收运行记录

2026-10-19：实现已进入最终验收，尚不标记 V2 完成。所有实际结果须同时满足全量正确性、真实分布式链路、至少 15 分钟正常负载以及端到端 P95≤10 秒；最终审核也须通过。

## 资源与部署配置

复用既有 Hadoop 3.1.3、Kafka 3.7.0、Java 8u212，隔离 CPython 3.11.16、Spark 3.5.6 / Scala 2.12。流任务两个单核 executor，各 512MiB heap +512MiB overhead，AM heap512MiB（YARN取整后容器1024MiB，两个executor各1024MiB，共3072MiB/3vcore；client driver另计），RocksDB 共享有界原生缓存128MiB；JVM class-space/code-cache 各64MiB。保持 YARN 内存校验，不修改全局配置。历史任务采用既有批处理脚本资源。机器为同一宿主上的三个节点，跨节点成功不能证明宿主故障容灾。

worker operator JSON 支持：

| 配置 | 默认 | 最终验收部署 |
| --- | --- | --- |
| `max_offsets` | 1000 | 全量100000；正常小样本1000 |
| `shuffle_partitions` | full24、small2 | 全量24；正常小样本显式small_shuffle_partitions=1 |
| `cursor_flush_records` | 1 | 100 |
| `cursor_flush_seconds` | 1 | 1 |
| `wait_for_stream_ready` | false | 正常延迟验收true |
| `audit_ack_log` | false | 正常延迟验收true |
| `observation_interval_seconds` | 1 | 正常延迟验收0.1 |
| `rocksdb_changelog` | false | 正常延迟验收true；全量突发保持原false |

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
- 随后以新隔离轮次 `v2-58e8ae0622ec45269dd5e147cb226a3f` 使用24分区重做完整链路。该轮次真实全链路已成功，流应用0018和历史应用0019均 FINISHED/SUCCEEDED；新发布历史与实时417个 UTC 行为桶逐一一致。实际 Chromium 完整页面1项测试通过11.9秒（含两种消息计数卡），详见 [永久证据](../evidence/v2-acceptance/README.md)。完整注册数据与 V1 来源校验一致，原始 V1 结果和topic均未修改。
- 实际边缘 fsync1000次在并行全量负载下：每条落盘11.289秒，100条批量落盘0.102秒。实际完整生产者约2500–2700条/秒；请求速率100000仅为上限，不是已实现吞吐或10秒延迟承诺。全量突发积压与另行5条/秒正常延迟验收分别报告。
- 容量观察：edge根盘36GiB中15GiB空闲，HDFS105.1GiB中55.0GiB可用、7.7GiB使用；不执行自动清理。

全量、完整浏览器及15分钟正常输入均已通过；最终审核仍待完成，当前不标记 V2 完成。


120秒最终观察路径预检的607条普通窗口 ACK 全部匹配首次页面绘制，但P95保守上界13621.7ms，未达到10秒；原始证据保留，未直接开始900秒。故意关闭浏览器15秒测试独立通过，后台源游标629→712、提交量610→682，重新打开同轮次后结果可见，提前停止后751条消息排空且旧完整历史保持不变。后续优化使用上述0.1秒驱动观察与0.25秒后台等待，并删除旧 Spark 样本收集动作；后续预检及900秒正式结果见下表，保留失败结果且未下调目标。


增量检查点单项验证使用 Spark 3.5.6 官方支持的 `spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled=true`（[官方说明](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html#rocksdb-state-store-changelog-checkpointing)）。它把本批状态变化持久化并周期生成后台快照，不改变精确去重或提交 offset 的一致性。保持2状态分区、原始输出布局、资源和调度配置不变后，状态提交由约800–1400ms降到约90–250ms，warm trigger约2.8–4秒。该值是作业诊断，不替代实际浏览器P95。

真实同轮恢复已通过：`v2-6af43e90fc5e4ff0a3d997e639c5ee1e` 应用0023→0024，ACK200后游标199、物理消息203；同一已有状态的检查点手动恢复后，359唯一有效源记录对应363物理消息、精确重试4，积压0，提前停止不发布部分历史。新 profile 固定检查点模式；旧未记录模式的 profile 归一为false。该故障阶段独立报告，不计入正常性能。首次尝试另遇新topic元数据传播暂不可见，cursor0且无Spark；新增仅启动创建阶段的有界等待并保留失败目录，观察/恢复仍对缺失topic报错。


## 正式900秒结果（通过，最终审核待完成）

最终部署 `e8090f6`，实际轮次 `v2-716a15738eae4aca89399b0c37f3a483`，YARN应用 `application_1792382297034_0028`。固定10000条输入以5条/秒持续生产，实际ACK跨度902.971718492秒、速率4.97358条/秒。4492条实际ACK全部匹配首次绘制；包括第一条数据及冷状态提交，不剔除延迟样本。P50保守上界6232.42ms、P95 **8580.68ms**、P99 10169.07ms、最大14720.43ms；规格要求P95而非每条≤10秒。时钟校准偏移区间跨度233.293288ms已计入上界。

每分钟积压中位数首29、末25，差-4，回归斜率-0.004464条/秒；没有持续增长积压。实际前端轮询中位2.01359秒、P95 2.01634秒。停止后4492条全部提交、lag0、stream_released=true、无错误、complete=false，完整已发布历史仍为全量轮次。全部原始ACK、绘制帧、逐条延迟、校准候选、轮询、观察、资源与配置哈希见 [永久原始证据](../evidence/v2-acceptance/normal-900s/summary.json)。

| 实测阶段 | 全部ACK | P95保守上界ms | 结论 |
| --- | ---: | ---: | --- |
| 修复回调、原观察路径 | 607 | 13621.71 | 失败，另行浏览器关闭测试通过 |
| API0.25s/driver0.1s、去多余collect | 611 | 12274.03 | 失败 |
| 单项changelog | 609 | 11358.67 | 失败 |
| 原始输出coalesce2 | 611 | 10815.50 | 失败 |
| 新轮次small状态1、300秒预检 | 1509 | 9476.15 | 仅短测，duration不足900故accepted=false |
| 独立正式900秒同profile | 4492 | 8580.68 | 通过全部正式门槛 |

启动正常profile必须同时设置 `small_shuffle_partitions: 1`、`rocksdb_changelog: true`、`observation_interval_seconds: 0.1`、`wait_for_stream_ready: true`、`audit_ack_log: true`、`cursor_flush_records: 100`、`cursor_flush_seconds: 1`，API `--poll-seconds 0.25`；前端保持2秒。专属空topic准备完成后才开始生产，全部第一批数据ACK均计入。这是在上述资源、状态规模和5条/秒正常输入下的验收结果，不是完整2.7M状态下同延迟或约2785条/秒突发吞吐的延迟承诺。全量已单独用24分区/原checkpoint模式核验正确性，不能把状态1用于全量或改变现有检查点分区。

正式资源观察的三节点整体CPU忙碌率分别约46.31%、16.20%、14.24%，包含原有Hadoop/Kafka服务；计算使用/proc/stat起止差，不把进程生命周期ps百分比称作窗口平均。驱动与executor RSS为时点观察，不声称峰值。原始资源数据与定义见 `normal-900s/resources-summary.json`。YARN应用0028最终FINISHED/SUCCEEDED，实际耗时946.967秒。
