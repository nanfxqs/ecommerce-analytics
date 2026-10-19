# V2 最终双轴审查与回归

2026-10-19，基线 `2ac658705e13b7dce009598ad442b58bec03deef`（V1 验收完成），审查集成提交 `4e4191c`。比较命令：`git diff 2ac6587...HEAD`。两个独立审查者分别检查 Standards 与 Spec，结论分开记录。

## Standards

Standards review — `2ac658705e13b7dce009598ad442b58bec03deef...HEAD` (`4e4191c`)。

已记录标准违例：**0**。依据：AGENTS.md / RTK.md、docs/agents/domain.md、GLOSSARY.md、ADR 0001–0003、README.md、TDD SKILL.md / tests.md / mocking.md。遵守已确认的 HTTP query/control、public batch/publication、captured pipeline 和 external worker CLI seams；未把外部边界替身本身列为违例。

Fowler baseline：**1 个 possible heuristic，P3，非硬规则违例**。

- **Duplicated Code — 事件清洗规则应有一个共享表达式。** 新增 `retailrocket/spark_live.py:13–21` 的 `_classified` 与新增 `retailrocket/spark_stream.py:168–175` 重复 UTC 时间戳范围转换及 invalid_timestamp / missing_visitor / missing_item / invalid_event 的优先级；同一规则已存在于 `retailrocket/spark_batch.py:53–60`。增加一个合法行为类型或调整异常优先级时必须同步三处，否则实时与历史质量/有效事件结果会分歧，破坏 README 要求的同输入核对。建议提取一个仅负责 DataFrame 事件分类的共享函数，保留独立的实时聚合和历史重算；既有固定输入测试继续独立提供期望值。

全部 baseline 均已考虑：Mysterious Name、Duplicated Code、Feature Envy、Data Clumps、Primitive Obsession、Repeated Switches、Shotgun Surgery、Divergent Change、Speculative Generality、Message Chains、Middle Man、Refused Bequest。其余未发现足够具体且可落地的依据，不作风格性报告。

只读静态审核；阅读了提交清单与非证据/vendor 的新增生产代码、测试及运行脚本，未运行测试、未访问集群。证据日志/vendor 不审查。

Resolution — `366dcd5`（修复 `76e2747`，EOF `dc766d2`）

唯一 P3 Duplicated Code finding **resolved**。只读核对 `git diff 4e4191c..HEAD -- retailrocket`：三条 pipeline 均改用 `retailrocket/event_classification.py:5` 的 `classify_events`，共享原 SQL 时间范围、四项异常及其 when 优先级；去重、聚合及 cache 的相对位置保留。该修复范围未发现新增 standards 问题。当前未解决明确标准违例 0；未解决 baseline smell 0。未重跑测试或访问集群；测试结果引用 root 提供的公开 stream/batch 比较与 live checkpoint 恢复 2 tests passed（35.04s），此前全套 81 passed。

## Spec

0 findings against #9 (`v2-spec`) for `git diff 2ac6587...HEAD` at 4e4191c.

(a) Missing/partial requirements: none confirmed. Reviewed durable single-run control, pause/drain/stop, restart/manual recovery, checkpoint corruption protection, exact source deduplication and quality accounting, committed offset/result alignment, staged automatic publication, guarded manual cleanup, and preservation of V1 historical queries and interactions.

(b) Scope creep: none confirmed. Recovery/profile guards, ACK tracing, calibrated latency instrumentation, acceptance scripts and retained failure evidence support explicitly requested recovery, integrity and acceptance requirements.

(c) Apparently implemented but incorrect requirements: none confirmed from the reviewed code and evidence. In particular, the spec requires “正常负载业务结果可见延迟 P95 不超过 10 秒、连续运行至少 15 分钟且积压不持续增长”; the independent normal profile records 902.9717 seconds, all 4492 ACKs matched, conservative P95 8.5807 seconds and no sustained backlog growth. The separate full burst validates 2,756,101 records and all 417 daily buckets through real cross-node stream → batch → publication. The spec does not impose the normal-profile latency bound on the full burst.

Reviewed the supplied commit list, #9 body, relevant glossary/ADRs, implementation and retained control/recovery/publication/acceptance evidence. Read-only inspection; no tests or cluster operations performed. This is a Spec-axis review conclusion, not a new execution of acceptance or a claim that unobserved failures are impossible.

## 修复与最终验证

唯一 P3 重复分类问题由同一修复者在 `76e2747` 提取为 `retailrocket.event_classification.classify_events`，`dc766d2` 修复 EOF 空白，合入集成提交 `366dcd5`。原审查者复核确认已解决且未引入新问题。SQL 表达式、UTC 时间戳边界、异常原因优先级及聚合/cache/去重位置均保留；没有新增 Spark action、shuffle、UDF 或状态操作，因此没有重跑已通过的 900 秒性能窗口。

- 集成提交 `4e4191c` 全量 Python 回归：81 passed，192.89 秒；仅第三方 Starlette/AnyIO 弃用提示。
- 分类等价提取后的公开 captured stream（含独立 batch 对照）与 exact live 检查点恢复回归：2 passed，35.04 秒；测试期望未改。
- TypeScript/Vite 生产构建通过，9.73 秒；构建仍提示主 bundle 大于 500kB。
- 实际 Chromium 历史日期/空结果及全部历史页测试：2 passed，8.8 秒。
- 实际已发布全量历史与实时结果浏览器测试：1 passed，32.6 秒，包含原始物理消息量/源游标两卡及 reload；复验时重启只读 API 后恢复成功，原始日志保留连接就绪前的重试。
- 正式性能证据：连续 902.9717 秒、4492/4492 全部实际 ACK 首次绘制匹配、P95 保守上界 8.5807 秒、积压无持续增长。适用范围为记录的小样本正常 profile、约 5 条/秒和对应资源；全量突发正确性另行验收，不作相同延迟承诺。

原始最终回归日志与本文件同目录。完整原始性能、跨节点、全量精确统计、恢复和失败证据继续保留。Standards 未解决：0；Spec 未解决：0。V2 已完成并通过验收。
