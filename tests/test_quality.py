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
        from retailrocket.query import anomaly_records, anomaly_reasons
        assert anomaly_reasons(tmp_path) == [
            {'invalid_reason': 'invalid_event', 'count': 1},
            {'invalid_reason': 'invalid_timestamp', 'count': 1},
            {'invalid_reason': 'missing_item', 'count': 1},
            {'invalid_reason': 'missing_visitor', 'count': 1}]
        assert [row['source_line'] for row in anomaly_records(tmp_path, limit=2)] == [4, 5]
        assert [row['source_line'] for row in anomaly_records(tmp_path, '2015-05-03', '2015-05-03')] == [5, 6, 7]
        assert anomaly_reasons(tmp_path, '2015-05-03', '2015-05-03') == [
            {'invalid_reason': 'invalid_event', 'count': 1},
            {'invalid_reason': 'missing_item', 'count': 1},
            {'invalid_reason': 'missing_visitor', 'count': 1}]
        retry = analyze(raw, {'batch_id': 'fixture', 'rows': 8, 'input': {'rows': 7}}, str(tmp_path), reuse_raw=True)
        assert retry == result
        assert spark.read.parquet(str(tmp_path / 'raw')).count() == 8
        assert spark.read.parquet(str(tmp_path / 'quarantine')).count() == 4
        assert overview(tmp_path / 'events', '2015-05-03', '2015-05-03') == {'view': 2, 'addtocart': 1, 'transaction': 0}
    finally:
        spark.stop()


def test_complete_batch_quarantines_timestamp_overflow_and_unsupported_years(tmp_path):
    from pyspark.sql import SparkSession
    from retailrocket.spark_batch import analyze
    from retailrocket.query import anomaly_reasons, anomaly_records
    spark = SparkSession.builder.master('local[2]').appName('timestamp-contract-test').config('spark.sql.shuffle.partitions', '2').getOrCreate()
    # Independent UTC millisecond boundaries for calendar years 1 through 9999.
    timestamps = ['-62135596800000', '253402300799999', '-62135596800001', '253402300800000', '9223372036854775807', '-9223372036854775808']
    rows = [dict(timestamp=timestamp, visitorid='1', event='view', itemid='2', transactionid='', source_file='events.csv', dataset_version='fixture-v1', batch_id='calendar', source_line=index, source_record_id=str(index)) for index, timestamp in enumerate(timestamps, 2)]
    raw = spark.createDataFrame([(json.dumps(row),) for row in rows], 'value STRING')
    try:
        quality = analyze(raw, {'batch_id': 'calendar', 'rows': 6, 'input': {'rows': 6}}, str(tmp_path))
        assert (quality['valid_count'], quality['invalid_count']) == (2, 4)
        assert anomaly_reasons(tmp_path) == [{'invalid_reason': 'invalid_timestamp', 'count': 4}]
        assert [row['source_line'] for row in anomaly_records(tmp_path)] == [4, 5, 6, 7]
        assert overview(tmp_path / 'events', '0001-01-01', '9999-12-31') == {'view': 2, 'addtocart': 0, 'transaction': 0}
    finally:
        spark.stop()
