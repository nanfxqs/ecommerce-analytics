# 离线手动清理 V2 回放轮次

系统默认保留所有轮次结果、检查点、日志及历史分析批次，不按时间自动删除，也没有页面删除功能。仅在不再需要该轮恢复、核对或演示证据后，由操作者使用以下公开命令。清理是不可逆操作；默认命令只显示计划，加入 `--apply` 才执行。

先关闭本机 API 服务，等待 edge worker 退出及 YARN 集群空闲。关闭浏览器不会停止 worker；应先通过页面停止或正常完成轮次。命令取得本机 supervisor 锁、edge worker 锁，检查终态、记录的 worker/stream PID 已退出及 YARN 没有活动应用。活动、暂停、待恢复、状态未知、检查点归属不明、锁占用和集群查询失败均拒绝删除。失败轮次只有在进程已退出且 YARN 可确认空闲时才可清理；完成或提前停止的轮次还须已有流资源释放确认。

在 edge 部署包含 `retailrocket.cleanup` 的同一版本代码；复用启动 API 时的本机 cluster 配置。该配置指向 edge 的 Python、代码、运行时和 edge 配置；edge 配置的 root 必须为实际轮次目录。不能使用本机路径代替 edge 路径。

```bash
uv run python -m retailrocket.cleanup \
  --run-store /operator/replay-runs \
  --store /operator/published \
  --cluster-config /operator/local-cluster.json \
  --run-id v2-0123456789abcdef0123456789abcdef
```

将上述示例占位值替换为已有的 `v2-` 加 32 位十六进制轮次 ID。查看 JSON 计划后，执行相同命令并增加 `--apply`：

```bash
uv run python -m retailrocket.cleanup \
  --run-store /operator/replay-runs \
  --store /operator/published \
  --cluster-config /operator/local-cluster.json \
  --run-id v2-0123456789abcdef0123456789abcdef \
  --apply
```

必须使用真实存在且确认要清理的轮次 ID；示例 ID 不对应任何运行结果。未知 ID、路径穿越、任意祖先或产物树内的符号链接都会拒绝。当前已发布批次的所有 revision 均受保护，不能只删除当前批次的旧 revision。命令完整校验发布指针与元数据身份；指针损坏，或已有批次却缺失发布指针时，先修复发布状态，不能绕过保护。

执行顺序为：

1. 本机取得 supervisor 锁，核对轮次及当前发布快照，保持锁直到完成。
2. 通过 SSH 调用同一模块的 edge helper。helper 再检查轮次、PID、锁、YARN 与 HDFS 目录归属；只删除本轮规范位置的 HDFS `streams/<run-id>`、`stream-checkpoints/<run-id>`、`batches/<run-id>`，以及 edge `root/<run-id>`。其中导出的历史结果、日志与发送进度都属于本轮目录。
3. edge 返回匹配本轮的完成收据后，本机才删除该轮镜像、该轮非当前发布批次及其批处理状态。如果本机 current 指向此已结束轮次，同时移除该指针，下一次启动形成新轮次。其他轮次和已发布当前批次不变。

edge 在 `root/.cleanup-receipts` 保留清理收据，避免 SSH 连接中断后丢失已完成清理的依据。HDFS 删除失败时保留 edge 和本机状态；中途失败后修复访问问题，再运行同一命令。不要手工删掉收据、修改 current 或清空目录来绕过拒绝。HDFS 使用普通删除而非 `-skipTrash`，实际垃圾箱保留由既有 HDFS 配置决定。

数据集源文件、准备后的输入、共享参考数据、Kafka topic 及 V1 已有产物不在此命令的清理范围。Kafka topic 继续遵循既有保留设置；本命令不提供无保护的 topic 删除或任意路径删除选项。存储空间计划必须另外计入这些保留数据。

验证边界：公开 CLI 测试使用隔离临时目录和注入的 SSH/HDFS/YARN 外部命令，覆盖真实本机文件删除、当前发布快照保护、活动/不确定状态及失败保留。此测试不代表在真实 HDFS 删除了任何验收结果。
