# 同轮次故障恢复与检查点保护实测

2026-10-19，专用轮次 `v2-a57db7c6eae7441e9507bf2125832ac5` 使用已校验 61 条固定样本、1 条/秒，真实本机 HTTP → SSH → Kafka → YARN/HDFS。未修改已有 V1 批次或其他探测命名空间。

1. 专用操作者配置 `audit_crash_after_ack=35`，在第 35 个真实 Kafka ACK 后、源游标写入前以 `os._exit(86)` 中断 worker。持久 ACK 游标为 34，Kafka/提交结果为 35。等待已提交结果后，对验证身份所属的 Spark driver 发送 SIGTERM，实际中断 worker 和 driver。
2. 全部 writer 停止后备份该轮次 HDFS 检查点 metadata，将它替换为 `{broken`。重启本机 API 后显示等待手动恢复；HTTP 恢复返回 503，原轮次 ID 保持不变，损坏字节未被删除或重建。见 [corruption.json](corruption.json)。
3. 操作者在 writer 全部停止期间明确恢复原 metadata 备份；这是本次故障实验的修复操作，系统没有自动重建检查点。随后再次启动本机 API，实际 Chromium 点击“恢复此轮次”，沿原 topic、原 HDFS output/checkpoint 和 ACK 游标继续。
4. 最终物理消费 62、唯一源记录 61、有效 61、异常 0、源重试 1、疑似内容重复 0，业务仍为浏览 59、加购 2、购买 0、独立访客 60，与该样本独立 V1/源数据口径一致。最终源游标 61、积压 0、stream_released=true。见 [recovered.json](recovered.json)。

Chromium 完整手动恢复检查通过（66.4 秒），没有模拟 HTTP 或集群。HTTP 集成另覆盖 API 重启后等待人工恢复、存活 worker 暂停协调、同轮限制、损坏检查点报错，以及既有控制和参数验证，共 6 项通过。前端构建及差异检查通过。本记录不将故障阶段时间混入正常负载延迟 P95，不代替 15 分钟或全量验收。

复现需准备独立 edge 代码/配置与固定小样本，启用上述一次性审计开关；等待 worker 中断后保存 ACK 游标、Kafka ends 和提交结果，再停掉**经过 boot ID/进程启动时刻核对的本轮 driver**。检查点损坏实验必须只操作这个停止的专用轮次，并在失败证明完成后明确恢复备份。浏览器测试 `REPLAY_RECOVERY_LIVE=1` 使用相应实际 API 与故障前 JSON 证据，默认不会在普通测试中运行。运行接口及恢复保护见 [回放运行说明](../../operations/replay.md)。
