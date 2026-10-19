"""Run with spark-submit on YARN; all distributed transformations use JVM SQL."""
import argparse
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F


def run(replay_path, brokers, output):
    replay = json.loads(Path(replay_path).read_text())
    if not replay.get('replay_confirmed'):
        raise ValueError('Replay is not confirmed')
    spark = SparkSession.builder.appName('retailrocket-' + replay['batch_id']).getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    if spark.sparkContext.master != 'yarn':
        raise ValueError('Distributed batch requires YARN')
    raw = (spark.read.format('kafka').option('kafka.bootstrap.servers', brokers)
           .option('subscribe', replay['topic']).option('startingOffsets', json.dumps(replay['startingOffsets']))
           .option('endingOffsets', json.dumps(replay['endingOffsets'])).option('failOnDataLoss', 'true').load())
    raw.write.mode('overwrite').parquet(output + '/raw')
    raw = spark.read.parquet(output + '/raw')
    schema = 'timestamp STRING,visitorid STRING,event STRING,itemid STRING,transactionid STRING,source_record_id STRING,source_line LONG,source_file STRING,dataset_version STRING,batch_id STRING,replay_time STRING'
    events = raw.select(F.from_json(F.col('value').cast('string'), schema).alias('event_record')).select('event_record.*')
    count = events.count()
    if count != replay['rows'] or events.filter(F.col('batch_id') != replay['batch_id']).count():
        raise ValueError('Kafka batch count or identity mismatch')
    events = events.withColumn('event_time', F.expr('timestamp_millis(cast(timestamp as bigint))'))
    events.write.mode('overwrite').parquet(output + '/events')
    events.groupBy('event').count().write.mode('overwrite').parquet(output + '/overview')
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
    args = parser.parse_args()
    run(args.replay, args.brokers, args.output)
