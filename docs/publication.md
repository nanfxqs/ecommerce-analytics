# 成功批次与恢复

Kafka 是采集传输，已准备的 JSONL/原始 CSV 和 HDFS 的 `<output>/raw`
是重算依据。保存 input.json、events.jsonl、replay.json 与 HDFS raw；不要把
Kafka 无限保留作为恢复方案。Spark 使用相同 output 重跑时覆盖自己的工作结果，
不会往已经发布的指标追加。使用 `--retry-from-raw` 明确从已落盘输入重算；缺失时
报错，不能悄悄回退到已过期 Kafka offsets。

准备阶段可用 `retailrocket prepare ... --store exports --batch-id batch-1` 记录；
回放用 `retailrocket replay ... --store exports --batch-id batch-1` 记录起止与失败。
准备失败清理不完整的准备目录后重做；回放失败重新执行回放并使用新确认的
replay.json，旧的未确认回放不可发布。Spark 阶段状态和数量保存在执行 evidence
以及导出的 metadata 中。保留失败日志和这些记录以供追溯。

把 Spark/HDFS 结果完整导出到独立本地目录，包含平铺的 metadata.json（至少有
batch_id）及各结果 Parquet 子目录。metadata 的 quality、data_range 和源文件版本
字段由分析阶段生成，发布器不绑定具体指标集合。

```
retailrocket publish --store exports --replay replay.json --export downloaded-batch
retailrocket status --store exports
retailrocket status --store exports --batch-id batch-1
```

发布先验证确认回放、批次身份、每个 Parquet 可读及复制完整性，再将不可变副本存入
`exports/batches/<batch_id>/<content revision>/`，最后原子切换 current.json。
失败不会移动当前指针；首次尚未成功时明确报告 No successful batch。
原导出目录改动不会影响发布副本。同一内容再次 publish 使用同一副本，不追加数据；
修复失败后重复同一命令是发布重试入口。不要直接修改 batches 内文件；读取时检查
校验和，损坏将明确报错。保留旧版本供已经开始的查询使用。

看板每次计算只调用一次 `current_snapshot(store)`，随后对返回的 directory 执行
所有查询；显示 metadata 中 batch_id 和 data_range。缓存键须含不可变 directory
（包含 revision）及所有筛选条件，不能只用可变 store 路径。发布过程可通过
`publish_batch(..., validate=...)` 加入业务校验，通过 `before_switch` 设置发布前
故障演练。任何校验异常都保留此前可查询的成功批次。

生产命令 `retailrocket publish` 额外调用 Retailrocket 结果契约校验：必须完整导出
raw/events/quarantine/quality 及全部看板结果，源行范围完整且唯一，数量与回放、
质量摘要相符，事件和参考数据的版本及源校验和一致。通用 publish_batch API 通过
validate 回调扩展，不依赖指标清单。`scripts/export-batch.sh HDFS_BATCH DESTINATION
[SPARK_EVIDENCE_JSON]` 下载全部已完成的结果目录并平铺 metadata，输出目录必须新建。

Spark 提交命令传 `--store /driver/path/batch-state`（省略时默认 replay.json 同级
batch-state）；它记录采集、落盘、分析成功和失败，重算用同一 store。
各机器的 store 是本机文件目录，演示证据收集时一并保存各阶段 runs/<batch>.json。
