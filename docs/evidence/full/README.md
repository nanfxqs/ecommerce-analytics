# Full distributed acceptance — 2026-10-19

The entire official Retailrocket version2 events input passed through Kafka and was persisted on HDFS. A failed analysis was recovered from that same raw input on YARN, validated, exported and published. This is actual distributed execution; local semantic tests are separate evidence.

## Input, collection and results

Dataset version `kaggle-retailrocket-v2`, events SHA256 `3745aa83238b1e6d44d8fda209807899f420084398f94ddf745f3cbcfecbf9e7`, source lines2–2756102. [replay.json](replay.json) confirms all2,756,101 acknowledgements and exact half-open offsets for dedicated three-partition/three-replica topic `retailrocket-full-v2-20261019`: partition0 `[0,920007)`, partition1 `[0,917873)`, partition2 `[0,918221)`. [kafka-topic.txt](kafka-topic.txt) independently records the three leaders/replicas/ISR and unchanged final broker offsets.

| Check | Actual result |
|---|---:|
| Input / consumed / unique source / valid events | 2,756,101 each |
| Invalid events / collection retries | 0 / 0 |
| Suspected source content duplicates, retained | 460 |
| Views / addtocart / transactions | 2,664,312 / 69,332 / 22,457 |
| Distinct visitors | 1,407,580 |
| View / cart / purchase visitors | 1,404,179 / 37,722 / 11,719 |
| View∩purchase / cart∩purchase visitors | 11,291 / 10,576 |
| Category-associated / unknown events | 2,099,173 / 656,928 |
| Category coverage | 76.164589% |

Independent original-CSV checks used Python CSV counts and visitor sets, plus a DuckDB content-distinct query for the460 duplicates. [source-oracle.json](source-oracle.json) matches all published event counts, distinct visitors and intersections. Source UTC range is `2015-05-03T03:00:04.384Z` through `2015-09-18T02:59:47.788Z`; published metadata and Parquet bounds agree. [validated-report.json](validated-report.json) records the queries. Sample full-range profiles0 and1 also match independently scanned CSV event counts, active days, purchases and recency ([profile-source-oracle.json](profile-source-oracle.json)).

References were imported directly into HDFS:788,214 category snapshots projected from20,275,902 property rows, plus1,669 category-tree rows. Original-file hashes and binding to the exact events input are in [references.json](references.json). Known fixtures separately verify nearest non-future category association, unknown preservation and deterministic preferences.

## Actual YARN execution and recovery

First full attempt `application_1792359468585_0003`, production source `457fa8c`, read the confirmed Kafka ranges and persisted all raw rows. Eight result tables completed, but the analysis failed when a Python-created quality-summary DataFrame tried to launch an unavailable executor Python. It was never published. [state-failed-0003.json](state-failed-0003.json) is a compact failure summary; complete logs/state remain on h1.

Commit `1329248` replaced summary and metadata construction with JVM SQL literals. A deliberately missing executor Python regression failed before the fix (20.70s) and passed after it (29.16s). No executor Python installation or cluster configuration change was needed.

The same batch `full-v2-20261019` then succeeded with `--retry-from-raw`, application `application_1792359468585_0004`, source `1329248` (same production code integrated as `9d33db1`). It used the persisted Kafka-derived raw input, not a local-mode substitute. Actual completed tasks ran on hadoop01 and hadoop02. ResourceManager reports `FINISHED/SUCCEEDED`. [spark-success.json](spark-success.json) and [cluster-success.txt](cluster-success.txt) record provenance, counts, HDFS file totals and task hosts.

All eight previously completed table row counts and unordered row-hash fingerprints were unchanged after recovery, including events and visitor/category summaries. [rerun-comparison.json](rerun-comparison.json) records this comparison; it does not claim that the failed attempt completed quality/overview/metadata. The final publication validator additionally reconciled all six aggregate-table multisets against authoritative events. The complete known-batch raw-rerun contract is covered by the integration test.

## Publication and dashboard

The public `retailrocket publish` command validated the complete export with source/reference provenance, source-line completeness, UTC metadata bounds, quality counts and aggregate contents. Initial successful revision was `a02d179ad84639104b7b46831d9351af2e6091cde3268a96a4b8f8bede289cee`.

A controlled pre-switch failure used a distinct valid candidate revision, created by copying the export and adding a clearly identified acceptance marker to metadata; analysis data was unchanged. During failure the old revision and query totals stayed available. Retrying actually switched to `ea9a9fb0f17d8e462a16464ac77368ade707fc15fc8dbcf7d6b69ba7fd6c4a3c`, with identical totals. [failure-retry-report.json](failure-retry-report.json) records before/candidate/during/after revisions and states. Reproduce with [verify-publication-recovery.py](../../../scripts/verify-publication-recovery.py).

Streamlit AppTest executed the final published full snapshot: overview, purchase associations, products/categories, profiles and data quality all completed without exceptions using one shared UTC date range. Batch/UTC captions, full metrics, positive visitor lookup, missing visitor and out-of-range pagination passed ([dashboard-report.json](dashboard-report.json)). Known fixtures cover UTC boundaries, non-sequential purchases, cross-day distinct visitors, zero denominators, source retry deduplication, valid empty transaction IDs, invalid isolation, recency and preference ordering.

Root's frozen full suite on integration `9d33db1` passed28 tests in160.14s, including the workerless regression and metadata/aggregate validation guards. A focused followup extended the known fixture with an empty day inside its valid date bounds; all five pages kept the same selected UTC day, overview displayed zeros/no data, and zero-denominator purchase rates displayed no data. This integration test passed in37.77s. No local test is represented as distributed evidence.

## Reproduce and inspect

Use the maintained [README commands](../../../README.md) for download, environment audit, full/sample preparation, references, acknowledged replay, YARN submission, completed export, validated publication, raw retry and dashboard. Exact run parameters: batch/topic `full-v2-20261019`, HDFS output `hdfs://mycluster/retailrocket/batches/full-v2-20261019`, HDFS references `hdfs://mycluster/retailrocket/references/full-v2-20261019`. The recovery command used the same paths and replay manifest with `--retry-from-raw`.

Current dashboard store on this machine: `/home/nanfl/.cache/retailrocket-published`.

```bash
uv run streamlit run retailrocket/dashboard.py -- /home/nanfl/.cache/retailrocket-published
```

Full input, exports and original logs are kept outside Git under host `/home/nanfl/.cache/retailrocket-full-*` and h1 `~/opt/retailrocket-full-*`. Edge source folders identify revisions (`retailrocket-full-code-457fa8c`, `retailrocket-full-code-1329248`). Execution logs/state are h1 `~/opt/retailrocket-full-v2/{replay.json,spark-failed-0003.log,retry.log,spark-evidence.json,state-failed-0003.json}`; driver registry is `~/opt/retailrocket-full-state`.

The isolated runtime is CPython3.11.16, Spark3.5.6/Scala2.12 and matching Kafka connector, using existing Java8u212/Hadoop3.1.3/Kafka3.7.0. Dependencies and connector checksums are pinned. Two1core executors use512MiB heap+384MiB overhead each; AM512MiB;24 shuffle partitions; per-job JVM class-space/code-cache caps64MiB preserve existing YARN memory enforcement. Existing cluster software/configuration was not replaced.

Three VMs share one physical host; this is a manual sequential coursework batch, not host-failure resilience or a performance guarantee. Guest wall clocks were8h behind host UTC; no clock was changed during jobs. Replay time records that observed clock. Historical event UTC comes from original epoch milliseconds and matches the independent source oracle.
