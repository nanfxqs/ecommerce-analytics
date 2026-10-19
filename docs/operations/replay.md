# 本机回放控制与集群 worker

后台 HTTP 是唯一控制入口，浏览器每 2 秒查询本机缓存；SSH、Kafka、YARN 和 HDFS 访问由后台协调，页面刷新不会启动 Spark。只运行一个 API worker，状态目录通过进程锁保护。关闭浏览器不停止 edge worker。

本机启动：

```bash
python -m retailrocket.api --store /operator/published --cluster-config /operator/local-cluster.json
```

本机配置由操作者提供，不接受页面上传：

```json
{"host":"h1","python":"/home/hadoop/opt/cpython-3.11.16-linux-x86_64-gnu/bin/python3.11","code":"/home/hadoop/opt/ecommerce-v2-runs","runtime":"/home/hadoop/opt/retailrocket-runtime","remote_config":"/home/hadoop/opt/ecommerce-v2-runs/edge.json"}
```

先将当前仓库的 `retailrocket` 和 `scripts` 复制到 edge 的 code 目录。edge 配置示例：

```json
{"root":"/home/hadoop/opt/ecommerce-v2-runs/runs","code":"/home/hadoop/opt/ecommerce-v2-runs","brokers":"hadoop01:9092,hadoop02:9092,hadoop03:9092","max_offsets":1000,"datasets":{"small":{"manifest":"/operator/prepared-small/input.json","label":"固定小样本","events_sha256":"<已验证的 events.jsonl SHA256>"},"full":{"manifest":"/operator/prepared-full/input.json","label":"完整数据","events_sha256":"<已验证的 events.jsonl SHA256>"}}}
```

数据注册只由操作者完成，启动前核对准备数据及原始源文件 SHA256、行数、连续源行、源身份。只允许固定小样本和完整数据，不能通过页面提交路径。生成轮次 ID 限定为 `v2-` 加 32 位十六进制；目录、topic、检查点及输出由后台生成，SSH 参数通过 argv/结构化 JSON 传递。

每轮 edge 保存 request、state、input、replay 及日志。本机保存 current 和每轮状态镜像。ACK 游标仅越过连续确认的源记录；正常结束生成的 replay.rows 按实际 Kafka offset 范围计算，不能与唯一源记录量混用。源重试需要继续沿用同一源记录身份。该阶段已保留恢复所需状态，故障后人工恢复控制由后续恢复任务实现，不能直接删除检查点重启。

页面分别显示 Kafka 当前独占末端、Spark 已提交检查点 offset 和持久结果覆盖 offset。检查点以 `commits/<batch>` 存在为依据，不能用先写出的 `offsets/<batch>` 或结果快照代替。driver 每秒通过已建立的 JVM Hadoop 客户端核对 HDFS 提交记录及对应结果，原子写出本轮本地 `stream-observation.json`。edge 观测只读取这个小文件和 Kafka 末端，不在每次查询中启动 HDFS CLI 或 Spark 作业。只有检查点与结果 offset 完全匹配的快照才进入业务页面；空分区明确按 0 处理。页面每 2 秒查询本机缓存，并分别显示观测时间与业务结果更新时间；端到端性能仍须独立验收。

正常 EOF 后等待所有发送确认，捕获最终 offset，等待检查点与结果都覆盖范围，然后向 driver 写入停止请求，等待 Spark 释放资源。完成状态表示回放范围处理完成，仍不是已发布历史分析；自动历史发布由后续任务接入。失败保留状态及日志，阻止另一个轮次抢占不确定资源；不要手工改 current 指针绕过保护。

控制 API：`GET /api/replay/datasets`、`GET /api/replay/current`、`POST /api/replay/runs`（dataset、正数 rate）。无活动轮次返回 null；并发启动 409，未知输入/非法速率 422。生产环境必须传入真实 cluster 配置；不配置时不会注册回放 API。

## 暂停、继续与提前停止

页面提供“暂停发送”“继续发送”“提前停止”。API 接受操作不等于处理已完成；页面区分已提交请求和后台实际确认。暂停会确认在途消息并保存连续 ACK 游标，随后停止新发送；Spark 持续消化 Kafka 积压。继续沿同一轮次和游标发送，不追赶暂停时间造成突发加速。提前停止会确认已发送数据、固定结束 offsets、等待检查点与结果覆盖、释放流任务，最终 `stopped`、`complete=false`、`early_stopped=true`。不调用历史发布。

命令行走同一 HTTP 控制入口，适合复现和排障：

```bash
python -m retailrocket.replay_control pause
python -m retailrocket.replay_control continue
python -m retailrocket.replay_control stop
```

`--url` 可指定本机 API 地址。对应 `POST /api/replay/current/pause`、`continue`、`stop`。新操作返回 202，重复已生效或在途操作返回 200 且 `duplicate=true`；不允许的状态返回 409。控制意图在本机保存后送达 edge 的 `control.json`，包含唯一 operation_id；worker 确认记录在 `last_control`。不允许在另一控制待确认时提交相反控制（停止除外）。观察结果的 generation 保护防止慢 SSH 响应覆盖较新的控制状态。故障时不能把“已接受”显示成“已完成”；后续恢复流程负责处理未确认意图。

## 中断后人工恢复

API 重启发现尚未结束的轮次时，先显示“等待手动恢复”。后台只核对远端状态，并请求仍在发送的同一 worker 暂停；不会自动继续发送或创建新轮次。浏览器关闭仍不影响健康运行的 API/worker。使用页面“恢复此轮次”或 `python -m retailrocket.replay_control recover` 明确恢复，期间不能另建轮次。

存活 worker 在确认暂停后继续同一发送进程；已中断 worker 由后台核对 PID、进程启动时刻和系统 boot ID，仅停止属于该轮次的旧 Spark writer，确认释放后接管原 topic、源 ACK 游标、输出及 HDFS 检查点。正常 EOF 或提前停止后中断只恢复原最终 offset 范围的消费，不重新发送全量。历史阶段失败使用历史发布重试操作，不能通过流恢复跳过校验。

发送端只在 Kafka 确认后推进连续源游标。确认与本地状态持久化之间故障可能重发少量源记录；源身份保持不变，最终指标精确去重。回放清单的物理消息量仍按原始开始与最终 Kafka offsets 计算，不能用唯一源行数替代。已有检查点缺失、JSON 损坏、主题/结果身份或引擎不匹配均明确拒绝恢复，保留原状态，绝不删除或重建检查点。

集群操作者的故障审计可在**专用验证配置**设置 `audit_crash_after_ack` 源序号，使独立 worker 在真实 ACK 后、游标写入前只中断一次；标记保存在轮次目录。该开关默认关闭、不接受页面参数，常规演示配置应移除。它用于证明重发语义，不能在正式数据轮次启用。此实现处理进程/服务中断；未宣称抵御整个宿主机故障。
