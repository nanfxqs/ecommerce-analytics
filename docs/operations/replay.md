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
