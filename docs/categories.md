# Historical category references

Use the exact event input manifest and dataset version for the reference import.
Only `categoryid` properties enter the analysis. Source filenames, row counts and
SHA256 checksums for both property parts and the preserved category tree are
registered in `references.json`, together with the event input manifest.

```sh
retailrocket prepare-references \
  --properties /data/item_properties_part1.csv /data/item_properties_part2.csv \
  --tree /data/category_tree.csv --output /data/prepared-references \
  --version retailrocket-checksum-snapshot --events-manifest /data/prepared/input.json \
  --hdfs-directory /retailrocket/references/retailrocket-checksum-snapshot
```

This uploads the filtered categories, unchanged tree and manifest directly through
`hdfs dfs -put`. Use a new HDFS directory for each immutable version. The behavior
source continues through Kafka. Set Spark batch's required `--references` argument
to this HDFS directory. The Spark job rejects version or source-checksum mismatch.
For local contract tests `analyze(..., references=directory)` accepts the same layout.

Spark joins each event to the category snapshot with the greatest timestamp less
than or equal to the event timestamp. Equal-timestamp duplicates select the lowest
category ID deterministically. Future snapshots never fill past events. Unmatched
events remain as `unknown`. The category tree is retained as reference provenance;
first-version analysis reports the original anonymous category IDs without parent
rollups or invented names.

Export `category_daily`, `item_daily`, `visitor_events`, `events`, `quality`,
`overview` and `metadata` along with the rest of the successful batch. Daily outputs
support inclusive UTC date-range queries via
`category_query.category_analysis(batch_directory, start, end)`. Category totals
include unknown events. Item totals include every valid event, including events
without a category. Purchases count source `transaction` events, not payment value.
The batch quality summary records matched/unknown counts and coverage. Range
coverage is recomputed from the selected daily aggregates. Empty ranges return
zero counts and undefined coverage, rendered as an explicit no-data message.
