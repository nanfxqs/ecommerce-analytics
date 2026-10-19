"""Run with spark-submit on YARN; all distributed transformations use JVM SQL."""
import argparse
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F



def associate_categories(events, references, replay):
    """Temporal interval join; unknown events survive and future records never fill."""
    from pyspark.sql.window import Window
    spark = events.sparkSession
    metadata = json.loads(spark.read.text(references + '/references.json', wholetext=True).first()[0])
    if metadata['dataset_version'] != replay.get('input', {}).get('dataset_version'):
        raise ValueError('Reference and event dataset versions differ')
    if metadata.get('events_input', {}).get('source_sha256') != replay.get('input', {}).get('source_sha256'):
        raise ValueError('Reference and event source checksums differ')
    properties = spark.read.option('header', True).schema('timestamp LONG,itemid STRING,categoryid STRING').csv(references + '/' + metadata['properties'])
    # A deterministic tie resolves duplicate snapshots without multiplying events.
    properties = properties.groupBy('itemid', 'timestamp').agg(F.min('categoryid').alias('categoryid'))
    properties = properties.withColumn('until', F.lead('timestamp').over(Window.partitionBy('itemid').orderBy('timestamp')))
    e, p = events.alias('e'), properties.alias('p')
    joined = e.join(p, (F.col('e.itemid') == F.col('p.itemid')) &
                    (F.col('e.timestamp').cast('long') >= F.col('p.timestamp')) &
                    (F.col('p.until').isNull() | (F.col('e.timestamp').cast('long') < F.col('p.until'))), 'left')
    return joined.select('e.*', F.coalesce(F.col('p.categoryid'), F.lit('unknown')).alias('categoryid')), metadata


def analyze(raw, replay, output, reuse_raw=False, references=None, tracking=None):
    """Persist one captured batch; usable locally for contracts, not acceptance."""
    raw.sparkSession.conf.set('spark.sql.session.timeZone', 'UTC')
    if not reuse_raw:
        raw.write.mode('errorifexists').parquet(output + '/raw')
    raw = raw.sparkSession.read.parquet(output + '/raw')
    if tracking:
        tracking.stage('collected', records=raw.count(), persisted_input=output + '/raw', retry_from_raw=reuse_raw)
        tracking.stage('analyzing')
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
    if references:
        valid, reference_metadata = associate_categories(valid, references, replay)
    else:
        valid = valid.withColumn('categoryid', F.lit('unknown'))
        reference_metadata = None
    invalid = events.filter(F.col('invalid_reason').isNotNull())
    valid_count = valid.count()
    contents = ['timestamp', 'visitorid', 'event', 'itemid', 'transactionid']
    suspected = unique_count - unique.select(*contents).distinct().count()
    quality = dict(input_count=replay.get('input', {}).get('rows', replay['rows']), consumed_count=consumed,
                   unique_source_count=unique_count, valid_count=valid_count, invalid_count=unique_count - valid_count,
                   retry_count=consumed - unique_count, suspected_duplicate_count=suspected)
    if references:
        associated = valid.filter(F.col('categoryid') != 'unknown').count()
        quality.update(category_associated_count=associated, category_unknown_count=valid_count-associated, category_coverage=associated/valid_count if valid_count else 0.0)
    valid.write.mode('overwrite').parquet(output + '/events')
    valid.select(F.to_date('event_time').alias('event_date'), 'visitorid', 'event').distinct().write.mode('overwrite').parquet(output + '/visitor_events')
    daily = valid.withColumn('event_date', F.to_date('event_time'))
    for key, name in [('categoryid', 'category_daily'), ('itemid', 'item_daily')]:
        daily.groupBy('event_date', key).agg(F.count('*').alias('events'), *[F.sum(F.when(F.col('event') == event, 1).otherwise(0)).alias(label) for event, label in [('view', 'views'), ('addtocart', 'addtocarts'), ('transaction', 'transactions')]]).write.mode('overwrite').parquet(output + '/' + name)
    from retailrocket.visitor_analysis import write_visitor_profiles
    write_visitor_profiles(valid, output)
    invalid.write.mode('overwrite').parquet(output + '/quarantine')
    raw.sparkSession.createDataFrame([quality]).write.mode('overwrite').parquet(output + '/quality')
    valid.groupBy('event').count().write.mode('overwrite').parquet(output + '/overview')
    bounds = valid.agg(F.min('event_time'), F.max('event_time')).first()
    metadata = dict(batch_id=replay['batch_id'], input=replay.get('input', {}), quality=quality,
                    references=reference_metadata, timezone='UTC', event_time_min=str(bounds[0]) if bounds[0] else None,
                    event_time_max=str(bounds[1]) if bounds[1] else None)
    raw.sparkSession.createDataFrame([(json.dumps(metadata),)], 'value STRING').coalesce(1).write.mode('overwrite').text(output + '/metadata')
    unique.unpersist()
    events.unpersist()
    return quality


def run(replay_path, brokers, output, retry_from_raw=False, references=None, store=None):
    try:
        from retailrocket.publication import BatchRun
    except ModuleNotFoundError:
        # spark-submit executes this file directly; its sibling is on sys.path.
        from publication import BatchRun
    replay = json.loads(Path(replay_path).read_text())
    tracking = BatchRun(store or Path(replay_path).parent / 'batch-state', replay['batch_id'])
    spark = None
    try:
        with tracking.tracking('collecting', replay=replay, output=output, retry_from_raw=retry_from_raw):
            if not replay.get('replay_confirmed'):
                raise ValueError('Replay is not confirmed')
            spark = SparkSession.builder.appName('retailrocket-' + replay['batch_id']).getOrCreate()
            spark.conf.set('spark.sql.session.timeZone', 'UTC')
            if spark.sparkContext.master != 'yarn':
                raise ValueError('Distributed batch requires YARN')
            if retry_from_raw:
                path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(output + '/raw')
                if not path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).exists(path):
                    raise FileNotFoundError('Persisted raw input is missing: ' + output + '/raw')
                raw = spark.read.parquet(output + '/raw')
            else:
                raw = (spark.read.format('kafka').option('kafka.bootstrap.servers', brokers)
                       .option('subscribe', replay['topic']).option('startingOffsets', json.dumps(replay['startingOffsets']))
                       .option('endingOffsets', json.dumps(replay['endingOffsets'])).option('failOnDataLoss', 'true').load())
            quality = analyze(raw, replay, output, reuse_raw=retry_from_raw, references=references, tracking=tracking)
            executor_hosts = str(spark.sparkContext._jsc.sc().getExecutorMemoryStatus().keys().toList())
            evidence = dict(batch_id=replay['batch_id'], rows=quality['consumed_count'], quality=quality,
                            application_id=spark.sparkContext.applicationId, master=spark.sparkContext.master,
                            executor_hosts=executor_hosts, output=output, retry_from_raw=retry_from_raw,
                            startingOffsets=replay['startingOffsets'], endingOffsets=replay['endingOffsets'])
            Path(replay_path).with_name('spark-evidence.json').write_text(json.dumps(evidence, indent=2))
            tracking.stage('analyzed', records=quality['valid_count'], quality=quality, evidence=evidence)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--replay', required=True)
    parser.add_argument('--brokers', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--references', required=True, help='HDFS reference directory containing references.json, categories.csv and category_tree.csv')
    parser.add_argument('--retry-from-raw', action='store_true')
    parser.add_argument('--store', help='Driver-local batch state registry')
    args = parser.parse_args()
    run(args.replay, args.brokers, args.output, args.retry_from_raw, args.references, args.store)
