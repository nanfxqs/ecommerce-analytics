# V2 final acceptance evidence (in progress)

The true full V2 chain passed on 2026-10-19. Final normal-load performance and final review are still pending; this directory does not declare V2 complete.

The dedicated run `v2-58e8ae0622ec45269dd5e147cb226a3f` used the registered official 2,756,101-row input, a NEW three-partition Kafka topic, HDFS output/checkpoint namespaces, an actual edge worker producer, incremental Structured Streaming, and automatic sequential historical processing/export/fetch/validated local publication. It did not consume the older V1 topic as a substitute for the V2 producer. V1 inputs/results remain unchanged.

- Stream application `application_1792382297034_0018`: FINISHED/SUCCEEDED, elapsed1,000.737s; executor tasks on hadoop02 and hadoop03, driverhadoop01.
- Historical application `application_1792382297034_0019`: FINISHED/SUCCEEDED, elapsed258.351s; actual completed executor tasks on hadoop01 and hadoop02.
- Final Kafka exclusive offsets: partition0=920007,1=917873,2=918221, all initially0. Confirmed/consumed/unique source/valid=2756101, invalid0, source retries0, suspected content duplicates460. Visitors1407580.
- Counts: view2664312, addtocart69332, transaction22457. Every one of417 live UTC day/event buckets equals the newly automatically recomputed historical result. The complete live metrics also match the previously independently verified V1 full oracle.
- HDFS stream420files/339396547logicalbytes; checkpoint1.4GiBlogical/2.8GiBreplicated at capture. `cluster.txt` retains actual command output. No automatic cleanup.
- Published revision `c4e1e04acf9d96e193f751743f3feedec7c7b57c8a4b785e566587cf5902cf1e`. Actual Chromium full-page test passed1/1 in7.7s: overview, purchase intersections, categorycoverage76.16%, profiles1407580, quality, final live metrics and reload.

`full-run.json`, `history.json`, `quality.json`, `daily-comparison.json`, `v1-comparison.json`, ResourceManager JSON and compressed actual Spark logs provide the counts and cross-node execution proof. `full-deployed-sha256.txt` pins the actual deployed sources/config: it includes cursor batching and configurable24partitions, and precedes the later local-only persisted-profile guard. The actual full configuration explicitly used24; no active imported source was changed during the jobs. Final normal performance must cover the final deployed observation path.

The full profile requests100000messages/s as an upper bound, with100000maxOffsetsPerTrigger,24shufflepartitions,100ACK/1s durable cursor flushing. Actual producer final average2785.19messages/s. This is intentional burst/backlog evidence, not a claim of10s visibility at that rate. Both512MiBheap+512MiBoverhead executors use a bounded128MiBshared native RocksDB cache; AM512MiB,1coreperexecutor, YARN memory checks enabled.

The first actual producer attempt used2statepartitions and failed after1516976confirmed records with RocksDB LRU-cache-full. It was not published. `failed-full-two-partitions.json` and the compressed failure log preserve that result; its topic, HDFS output and checkpoint remain isolated. A checkpoint's state partition count cannot migrate. The corrected registered-full default is24, while the small normal profile uses2; persisted profiles reject recovery with a different count.

The cursor benchmark is actual fsync in private temporary paths during full load:1000ACKs at one durable write each took11.289s, versus0.102s at100ACKs/write. Default1 remains for compatibility with the real ACK35/cursor34 crash test. Pause/stop/EOF flush pending ACKs and the cursor; uncertain source suffixes replay with exact identity deduplication.

Reproduction commands and the calibrated ACK→first painted offset protocol are in [the operations record](../../operations/v2-acceptance.md). Prior real fault coverage remains in sibling `v2-stream`, `v2-controls`, `v2-recovery`, `v2-publication` directories. Large runtime trees and preparation manifests remain on the edge under `/home/hadoop/opt/ecommerce-v2-acceptance`, and captured artifacts under `/tmp/ecommerce-v2-agent`; all permanent evidence here avoids requiring a local-mode substitute.
