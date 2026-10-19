# V2 增量业务指标与已提交观测

#14 的小样本行为、HTTP及浏览器证据；不是 V2 全量或性能验收完成声明。

真实 Kafka/YARN/HDFS worker 轮次 `v2-4af02cc63a584069b89c38ef31f61e4b` 正常完成并释放流任务。完整状态 [smoke.json](smoke.json)，匹配真实 Spark commit 的 driver 缓存 [committed-observation.json](committed-observation.json)。输入为官方源文件固定前61行，独立扫描得 [source-oracle.json](source-oracle.json)：59浏览、2加购、0购买、60访客，事件日期为UTC2015-06-02。增量状态得61有效、0异常/重试/疑似重复，分区独占末端25/18/18，积压0，符合相同输入的 V1 口径。该小样本使用最初64MiB原生缓存和384MiB overhead；后续全量压力暴露边界后，流任务默认调整为128MiB缓存、512MiB overhead，heap仍512MiB。

本地公开 captured-input→检查点→快照→HTTP 测试覆盖合法乱序、跨日访客精确去重、异常时间不计访客、源重试、不同源行相同内容，以及冲突源重试不增加访客。另通过真实文件权限故障，让业务快照已落盘后 Spark commit 写入失败；恢复同一检查点，完整消费状态但不重复累计外部结果。Kafka检查点观测的本地测试在外部文件系统边界提供真实v1日志格式，两条源分支的offset必须一致；没有commit时不展示结果。

实际 Chromium + 真实 HTTP 服务测试，固定源期望值核对事件、访客、质量、UTC趋势并刷新浏览器后再次核对。命令：`LIVE_METRICS=1 npm --prefix frontend run test:e2e -- --config playwright.live.config.ts`；需要运行该证据对应 API（8003端口）并使用固定61行小样本状态。测试不使用网页请求拦截或模拟业务查询。

[五次实际外部观测耗时](observation-timing.json) 包含 SSH、Kafka 末端查询和本地提交缓存读取，0.606–0.933秒；此前每次启动多个 HDFS CLI 约20秒。缓存由流 driver 每秒通过现有 JVM HDFS 客户端读取已提交checkpoint及精确匹配的业务快照后原子更新。这个耗时不是 Kafka确认到页面可见的端到端延迟，不能替代 P95 验收。

## 全量压力边界与继续验证

一次向512MiB heap任务灌入整个275万条微批次发生两类错误：共享64MiB RocksDB LRU缓存不足及巨大缓存分区触发Java heap OOM。探测在失败加重后主动停止，原始日志保留在edge隔离目录；[失败证据](scale-initial-failure.json)。没有修改既有全量topic或V1结果，不能把该次称为通过。

随后采用有界100000条微批次、每作业128MiB RocksDB缓存及512MiB executor overhead（保持512MiB heap、24shuffle、2个单核executor，YARN检查继续启用），在新namespace重新读取既有全量topic，并同时订阅一个新建空重试topic。前4批已提交约399992条输入，冷trigger39.685秒，其后三批约17–18秒/10万条；该截取时没有LRU错误、executor损失或OOM，见 [仍在运行的部分证据](scale-bounded-partial.json)。该数据不是完整总数证明，也不是持续正常负载10秒目标通过。

当前worker默认1000条微批次、2shuffle，生产者速率单独设置。全量状态增长、已填满状态下小批次更新及15分钟页面P95验收由后续完整验收继续，性能目标不降低。旧版累计重扫基线的125.994秒全量trigger见前置scale探索；新方案不再随每批扫描原始账本。完整验证完成前，不声明V2验收通过。
