"""Bounded-round streaming capture and recoverable, queryable cumulative results.

The append-only raw microbatch ledger is the exact full-round deduplication state.
It deliberately has no watermark: late valid source records survive. Recomputing
from this ledger trades CPU/I/O for bounded executor memory and simple recovery.
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

SCHEMA = ('timestamp STRING,visitorid STRING,event STRING,itemid STRING,transactionid STRING,'
          'source_record_id STRING,source_line LONG,source_file STRING,dataset_version STRING,'
          'batch_id STRING,replay_time STRING')


def _filesystem(spark, location):
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(location)
    return path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), path


def _exists(spark, location):
    fs, path = _filesystem(spark, location)
    return fs.exists(path)


def _directories(spark, location):
    fs, path = _filesystem(spark, location)
    if not fs.exists(path):
        return []
    return sorted(str(item.getPath()) for item in fs.listStatus(path) if item.isDirectory())


def _commit_text(spark, target, text):
    temporary = target + '.pending'
    fs, path = _filesystem(spark, temporary)
    if fs.exists(path):
        fs.delete(path, True)
    spark.range(1).select(F.lit(text).alias('value')).coalesce(1).write.text(temporary)
    _, final = _filesystem(spark, target)
    if fs.exists(final):
        raise ValueError('Refusing to replace committed output: ' + target)
    if not fs.rename(path, final):
        raise IOError('Unable to commit ' + target)


def read_snapshot(spark, output):
    """Read latest complete immutable result; never expose a partial result."""
    complete = [p for p in _directories(spark, output + '/snapshots')
                if not p.endswith('.pending') and _exists(spark, p + '/_SUCCESS')]
    if not complete:
        return None
    return json.loads(spark.read.text(complete[-1], wholetext=True).first()[0])


def commit_microbatch(raw, batch_id, output, run_id):
    """Public pipeline callback, idempotent across failure before Spark commit.

    Input is the captured Kafka value/topic/partition/offset frame. Snapshot
    offsets are exclusive and only advance after durable raw and result writes.
    """
    spark = raw.sparkSession
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    raw_path = output + '/raw/%020d' % batch_id
    snapshot_path = output + '/snapshots/%020d' % batch_id
    if _exists(spark, snapshot_path + '/_SUCCESS'):
        snapshot = json.loads(spark.read.text(snapshot_path, wholetext=True).first()[0])
        if snapshot['run_id'] != run_id:
            raise ValueError('Streaming output belongs to another replay run')
        return snapshot
    if not _exists(spark, raw_path + '/_SUCCESS'):
        fs, path = _filesystem(spark, raw_path)
        if fs.exists(path):
            fs.delete(path, True)  # Only this uncommitted microbatch, never committed input.
        raw.select(F.col('value').cast('string'), 'topic', 'partition', 'offset').write.parquet(raw_path)
    paths = [p for p in _directories(spark, output + '/raw') if _exists(spark, p + '/_SUCCESS')]
    captured = spark.read.parquet(*paths)
    records = captured.select(F.from_json('value', SCHEMA).alias('r'), 'topic', 'partition', 'offset').select('r.*', 'topic', 'partition', 'offset')
    bad_identity = (F.col('batch_id').isNull() | (F.col('batch_id') != run_id) |
                    F.col('source_line').isNull() | (F.col('source_line') < 2) |
                    F.col('source_file').isNull() | (F.trim('source_file') == '') |
                    F.col('dataset_version').isNull() | (F.trim('dataset_version') == ''))
    partition_stats = records.groupBy('topic', 'partition').agg(
        F.count('*').alias('consumed'), (F.max('offset')+1).alias('end'),
        F.sum(F.when(bad_identity,1).otherwise(0)).alias('invalid_identity')).collect()
    if any(r.invalid_identity for r in partition_stats):
        raise ValueError('Missing source provenance or incorrect replay run identity')
    unique = records.drop('topic', 'partition', 'offset').dropDuplicates(['dataset_version', 'source_file', 'source_line'])
    events = unique.withColumn('event_time', F.expr('''CASE
        WHEN try_cast(timestamp AS bigint) BETWEEN -62135596800000 AND 253402300799999
        THEN timestamp_millis(try_cast(timestamp AS bigint)) END'''))
    reason = (F.when(F.col('event_time').isNull(), 'invalid_timestamp')
              .when(F.col('visitorid').isNull() | (F.trim('visitorid') == ''), 'missing_visitor')
              .when(F.col('itemid').isNull() | (F.trim('itemid') == ''), 'missing_item')
              .when(F.col('event').isNull() | ~F.col('event').isin('view', 'addtocart', 'transaction'), 'invalid_event'))
    events = events.withColumn('invalid_reason', reason).cache()
    try:
        consumed = sum(r.consumed for r in partition_stats)
        contents = ['timestamp', 'visitorid', 'event', 'itemid', 'transactionid']
        summary = events.agg(F.count('*').alias('unique_count'),
            F.sum(F.when(F.col('invalid_reason').isNull(),1).otherwise(0)).alias('valid'),
            F.countDistinct(F.when(F.col('invalid_reason').isNull(),F.col('visitorid'))).alias('visitors'),
            F.countDistinct(F.struct(*contents)).alias('distinct_contents'),
            F.min(F.when(F.col('invalid_reason').isNull(),F.col('event_time'))).cast('string').alias('event_time_min'),
            F.max(F.when(F.col('invalid_reason').isNull(),F.col('event_time'))).cast('string').alias('event_time_max')).first()
        unique_count = summary.unique_count
        valid_count = summary.valid or 0
        suspected = unique_count-summary.distinct_contents
        offsets = {}
        for row in partition_stats:
            offsets.setdefault(row.topic, {})[str(row.partition)] = row.end
        groups = events.select(F.to_date('event_time').alias('event_date'), 'event', 'invalid_reason').groupBy('event_date','event','invalid_reason').count().collect()
        overview, daily, invalid_reasons = {}, [], {}
        for row in groups:
            if row.invalid_reason is not None:
                invalid_reasons[row.invalid_reason] = invalid_reasons.get(row.invalid_reason, 0)+row['count']
            else:
                overview[row.event] = overview.get(row.event, 0)+row['count']
                daily.append(dict(event_date=str(row.event_date), event=row.event, count=row['count']))
        daily.sort(key=lambda row:(row['event_date'],row['event']))
        snapshot = dict(run_id=run_id, microbatch_id=batch_id, timezone='UTC',
                        updated_at=datetime.now(timezone.utc).isoformat(), complete=False,
                        quality=dict(consumed_count=consumed, unique_source_count=unique_count,
                                     valid_count=valid_count, invalid_count=unique_count-valid_count,
                                     retry_count=consumed-unique_count, suspected_duplicate_count=suspected),
                        visitors=summary.visitors, event_time_min=summary.event_time_min,
                        event_time_max=summary.event_time_max,
                        overview=overview, daily=daily, invalid_reasons=invalid_reasons,
                        processed_offsets=offsets,
                        samples=[json.loads(r.value) for r in captured.orderBy(F.desc('offset'), 'partition').limit(20).collect()],
                        application_id=spark.sparkContext.applicationId, master=spark.sparkContext.master,
                        executor_hosts=str(spark.sparkContext._jsc.sc().getExecutorMemoryStatus().keys().toList()))
        _commit_text(spark, snapshot_path, json.dumps(snapshot, ensure_ascii=False))
        return snapshot
    finally:
        events.unpersist()


def start_stream(spark, *, brokers, topic, output, checkpoint, run_id,
                 starting_offsets='earliest', max_offsets=10000, trigger_seconds=2, available_now=False):
    """Start a resumable dedicated-topic query. Caller owns stopping/draining it."""
    if _exists(spark, output + '/raw') and not _exists(spark, checkpoint + '/metadata'):
        raise ValueError('Existing streaming output requires its original checkpoint; refusing silent restart')
    previous = read_snapshot(spark, output)
    if previous and previous['run_id'] != run_id:
        raise ValueError('Streaming output belongs to another replay run')
    frame = (spark.readStream.format('kafka').option('kafka.bootstrap.servers', brokers)
             .option('subscribe', topic).option('startingOffsets', starting_offsets)
             .option('failOnDataLoss', 'true').option('maxOffsetsPerTrigger', max_offsets).load())
    writer = (frame.writeStream.option('checkpointLocation', checkpoint)
              .foreachBatch(lambda batch, batch_id: commit_microbatch(batch, batch_id, output, run_id)))
    return writer.trigger(availableNow=True).start() if available_now else writer.trigger(processingTime=f'{trigger_seconds} seconds').start()


def main():
    parser = argparse.ArgumentParser()
    for option in ['brokers', 'topic', 'output', 'checkpoint', 'run-id']:
        parser.add_argument('--' + option, required=True)
    parser.add_argument('--starting-offsets', default='earliest')
    parser.add_argument('--max-offsets', type=int, default=10000)
    parser.add_argument('--trigger-seconds', type=int, default=2)
    parser.add_argument('--available-now', action='store_true')
    parser.add_argument('--stop-file', help='Driver-local graceful shutdown request')
    parser.add_argument('--evidence', help='Driver-local JSON evidence after query exits successfully')
    args = parser.parse_args()
    spark = SparkSession.builder.appName('retailrocket-stream-' + args.run_id).getOrCreate()
    try:
        if spark.sparkContext.master != 'yarn':
            raise ValueError('Distributed streaming requires YARN')
        if not args.checkpoint.startswith('hdfs://') or not args.output.startswith('hdfs://'):
            raise ValueError('Distributed stream results and checkpoints must use HDFS')
        query = start_stream(spark, brokers=args.brokers, topic=args.topic, output=args.output,
                             checkpoint=args.checkpoint, run_id=args.run_id,
                             starting_offsets=args.starting_offsets, max_offsets=args.max_offsets,
                             trigger_seconds=args.trigger_seconds, available_now=args.available_now)
        if args.stop_file:
            while query.isActive:
                if Path(args.stop_file).exists():
                    query.stop()
                    break
                query.awaitTermination(1)
            if query.exception():
                raise RuntimeError(str(query.exception()))
        else:
            query.awaitTermination()
        if args.evidence:
            Path(args.evidence).write_text(json.dumps(dict(snapshot=read_snapshot(spark,args.output),
                progress=query.recentProgress), indent=2))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
