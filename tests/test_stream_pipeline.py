"""Public pipeline seam: captured Kafka records -> durable streaming query result."""
import json
from pathlib import Path
import pytest


def test_stream_pipeline_retries_and_utc_visitor_scope(tmp_path):
    from pyspark.sql import SparkSession
    from retailrocket.spark_stream import commit_microbatch, read_snapshot
    spark = SparkSession.builder.master('local[2]').appName('stream-contract').config('spark.sql.shuffle.partitions', '2').getOrCreate()
    def message(line, timestamp, visitor='1', event='view'):
        return json.dumps(dict(dataset_version='known', source_file='events.csv', source_line=line,
                              batch_id='run', timestamp=timestamp, visitorid=visitor, event=event, itemid='a'))
    first = [(message(2, '86400000'), 'topic', 0, 0), (message(3, '0'), 'topic', 1, 0),
             (message(4, 'bad'), 'topic', 0, 1)]
    schema = 'value STRING,topic STRING,partition INT,offset LONG'
    try:
        commit_microbatch(spark.createDataFrame(first, schema), 0, str(tmp_path/'stream'), 'run')
        commit_microbatch(spark.createDataFrame(first, schema), 0, str(tmp_path/'stream'), 'run')
        second = [(message(2, '86400000'), 'topic', 0, 2), (message(5, '0'), 'topic', 1, 1)]
        commit_microbatch(spark.createDataFrame(second, schema), 1, str(tmp_path/'stream'), 'run')
        result = read_snapshot(spark, str(tmp_path/'stream'))
        assert result['quality'] == dict(consumed_count=5, unique_source_count=4, valid_count=3,
                                        invalid_count=1, retry_count=1, suspected_duplicate_count=1)
        assert result['visitors'] == 1
        assert result['overview'] == {'view': 3}
        assert result['daily'] == [dict(event_date='1970-01-01', event='view', count=2),
                                   dict(event_date='1970-01-02', event='view', count=1)]
        assert result['invalid_reasons'] == {'invalid_timestamp': 1}
        assert result['processed_offsets'] == {'topic': {'0': 3, '1': 2}}
        assert result['run_id'] == 'run'
    finally:
        spark.stop()
