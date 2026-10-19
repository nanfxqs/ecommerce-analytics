"""Run with spark-submit on YARN; all distributed transformations use JVM SQL."""
import argparse
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F



def analyze(raw, replay, output, reuse_raw=False):
    """Persist one captured batch; usable locally for contracts, not acceptance."""
    if not reuse_raw:
        raw.write.mode('errorifexists').parquet(output + '/raw')
    raw = raw.sparkSession.read.parquet(output + '/raw')
    schema = 'timestamp STRING,visitorid STRING,event STRING,itemid STRING,transactionid STRING,source_record_id STRING,source_line LONG,source_file STRING,dataset_version STRING,batch_id STRING,replay_time STRING'
    events = raw.select(F.from_json(F.col('value').cast('string'), schema).alias('record')).select('record.*')
    consumed = events.count()
    if consumed != replay['rows'] or events.filter(F.col('batch_id').isNull() | (F.col('batch_id') != replay['batch_id'])).count():
        raise ValueError('Kafka batch count or identity mismatch')
    # Identity is provenance, never the event contents or a producer supplied hash.
    identity = ['dataset_version', 'source_file', 'source_line']
    if events.filter(F.col('source_line').isNull() | (F.col('source_line') < 2) | F.col('source_file').isNull() | F.col('dataset_version').isNull()).count():
        raise ValueError('Missing source provenance')
    unique = events.dropDuplicates(identity).cache()
    unique_count = unique.count()
    events = unique.withColumn('event_time', F.expr('timestamp_millis(try_cast(timestamp as bigint))'))
    reason = (F.when(F.col('event_time').isNull(), 'invalid_timestamp')
              .when(F.col('visitorid').isNull() | (F.trim('visitorid') == ''), 'missing_visitor')
              .when(F.col('itemid').isNull() | (F.trim('itemid') == ''), 'missing_item')
              .when(F.col('event').isNull() | ~F.col('event').isin('view', 'addtocart', 'transaction'), 'invalid_event'))
    events = events.withColumn('invalid_reason', reason).cache()
    valid = events.filter(F.col('invalid_reason').isNull()).drop('invalid_reason')
    invalid = events.filter(F.col('invalid_reason').isNotNull())
    valid_count = valid.count()
    contents = ['timestamp', 'visitorid', 'event', 'itemid', 'transactionid']
    suspected = unique_count - unique.select(*contents).distinct().count()
    quality = dict(input_count=replay.get('input', {}).get('rows', replay['rows']), consumed_count=consumed,
                   unique_source_count=unique_count, valid_count=valid_count, invalid_count=unique_count - valid_count,
                   retry_count=consumed - unique_count, suspected_duplicate_count=suspected)
    valid.write.mode('overwrite').parquet(output + '/events')
    invalid.write.mode('overwrite').parquet(output + '/quarantine')
    raw.sparkSession.createDataFrame([quality]).write.mode('overwrite').parquet(output + '/quality')
    valid.groupBy('event').count().write.mode('overwrite').parquet(output + '/overview')
    bounds = valid.agg(F.min('event_time'), F.max('event_time')).first()
    metadata = dict(batch_id=replay['batch_id'], input=replay.get('input', {}), quality=quality,
                    timezone='UTC', event_time_min=str(bounds[0]) if bounds[0] else None,
                    event_time_max=str(bounds[1]) if bounds[1] else None)
    raw.sparkSession.createDataFrame([(json.dumps(metadata),)], 'value STRING').coalesce(1).write.mode('overwrite').text(output + '/metadata')
    unique.unpersist()
    events.unpersist()
    return quality


def run(replay_path, brokers, output, retry_from_raw=False):
    replay = json.loads(Path(replay_path).read_text())
    if not replay.get('replay_confirmed'):
        raise ValueError('Replay is not confirmed')
    spark = SparkSession.builder.appName('retailrocket-' + replay['batch_id']).getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    if spark.sparkContext.master != 'yarn':
        raise ValueError('Distributed batch requires YARN')
    if retry_from_raw:
        raw = spark.read.parquet(output + '/raw')
    else:
        raw = (spark.read.format('kafka').option('kafka.bootstrap.servers', brokers)
               .option('subscribe', replay['topic']).option('startingOffsets', json.dumps(replay['startingOffsets']))
               .option('endingOffsets', json.dumps(replay['endingOffsets'])).option('failOnDataLoss', 'true').load())
    quality = analyze(raw, replay, output, reuse_raw=retry_from_raw)
    count = quality['consumed_count']
    # Executor registration provides actual host allocation, not a local-mode claim.
    executor_hosts = str(spark.sparkContext._jsc.sc().getExecutorMemoryStatus().keys().toList())
    evidence = dict(batch_id=replay['batch_id'], rows=count, application_id=spark.sparkContext.applicationId,
                    master=spark.sparkContext.master, executor_hosts=executor_hosts, output=output,
                    startingOffsets=replay['startingOffsets'], endingOffsets=replay['endingOffsets'])
    Path(replay_path).with_name('spark-evidence.json').write_text(json.dumps(evidence, indent=2))
    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--replay', required=True)
    parser.add_argument('--brokers', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--retry-from-raw', action='store_true')
    args = parser.parse_args()
    run(args.replay, args.brokers, args.output, args.retry_from_raw)
