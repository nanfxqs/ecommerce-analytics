# Real distributed small-sample acceptance

Host date2026-10-19. This validates issue2's initial vertical slice at source revision `f65b8e5` (replay later used bounded asynchronous acknowledgements from `5491c4d`). Followup cleaning, reference enrichment, publication and full-data acceptance require their own latest-integration run.

Input: official Retailrocket dataset version2, original events SHA256 `3745aa83238b1e6d44d8fda209807899f420084398f94ddf745f3cbcfecbf9e7`, source lines2–10001, exactly10,000 rows. Independent CSV counts: view9661, addtocart250, transaction89.

The Python historical replay acknowledged all10,000 messages. Dedicated topic `retailrocket-sample-20261019`, starting offsets0/0/0, exclusive ending offsets3265/3425/3310. Exact replay manifest is `sample-replay.json`.

Spark3.5.6 on existing Java8u212/Hadoop3.1.3 read those exact offsets with the pinned Kafka connector and wrote HDFS raw, events and overview. Application `application_1792359468585_0002` finished `SUCCEEDED` in YARN. Tasks completed on both `hadoop01` (executor2) and `hadoop02` (executor1); AM was on `hadoop03`. This was actual distributed execution, not local mode. `sample-cluster.txt` records HDFS file listings, task host lines and the ResourceManager response. `sample-spark.json` records driver evidence.

Submission used `scripts/submit-batch.sh` resource values and job-only JVM reservation caps. The earlier application0001 failed YARN virtual-memory enforcement; it is not success evidence. Existing cluster configuration was preserved.

After HDFS export, DuckDB read actual Parquet events and returned the same9661/250/89 counts (`sample-query.json`). Streamlit AppTest executed the baseline dashboard against that export, without exceptions, and displayed matching three metrics (`sample-dashboard.json`).

Clock discrepancy: guest UTC was8h behind host UTC; logs show guest local time. No clocks were changed. Original event UTC derives source epoch milliseconds and is unaffected. `replay_time` is the observed guest wall clock.

Runtime/log/data pointers retained outside the repository for subsequent work: `/tmp/retailrocket-implementation-notes.md`, `/tmp/retailrocket-sample-export`, and h1 `~/opt/retailrocket-sample/{replay.json,spark.log,spark-evidence.json}`. Large input and output artifacts are intentionally excluded from Git.
