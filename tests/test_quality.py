"""Known batch -> persisted outputs; local Spark is not distributed acceptance."""
import json
from retailrocket.query import overview


def test_complete_batch_isolates_invalid_and_retains_source_duplicates(tmp_path):
    from pyspark.sql import SparkSession
    from retailrocket.spark_batch import analyze
    spark = SparkSession.builder.master('local[2]').appName('quality-contract-test').getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    spark.conf.set('spark.sql.shuffle.partitions', '2')
    base = dict(timestamp='1430622000000', visitorid='1', event='view', itemid='2', transactionid='', source_file='events.csv', dataset_version='fixture-v1', batch_id='fixture')
    events = [dict(base, source_line=i, source_record_id=str(i)) for i in range(2, 9)]
    events[2]['timestamp'] = 'bad'
    events[3]['visitorid'] = ''
    events[4]['itemid'] = None
    events[5]['event'] = 'unknown'
    events[6]['event'] = 'addtocart'
    messages = events + [events[0]]
    raw = spark.createDataFrame([(json.dumps(row),) for row in messages], 'value STRING')
    try:
        result = analyze(raw, {'batch_id': 'fixture', 'rows': 8, 'input': {'rows': 7}}, str(tmp_path))
        assert result == dict(input_count=7, consumed_count=8, unique_source_count=7, valid_count=3, invalid_count=4, retry_count=1, suspected_duplicate_count=1)
        from retailrocket.query import quality_summary
        assert quality_summary(tmp_path) == result
        retry = analyze(raw, {'batch_id': 'fixture', 'rows': 8, 'input': {'rows': 7}}, str(tmp_path), reuse_raw=True)
        assert retry == result
        assert spark.read.parquet(str(tmp_path / 'raw')).count() == 8
        assert spark.read.parquet(str(tmp_path / 'quarantine')).count() == 4
        assert overview(tmp_path / 'events', '2015-05-03', '2015-05-03') == {'view': 2, 'addtocart': 1, 'transaction': 0}
    finally:
        spark.stop()
