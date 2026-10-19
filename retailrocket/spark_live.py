"""Incremental full-round exact metrics backed by native checkpointed state.

Source provenance is deduplicated before content/valid-visitor identities. The
stateless message branch retains every raw Kafka message, including retries.
"""
from datetime import datetime, timezone
import json
from pyspark.sql import functions as F
from retailrocket.spark_stream import (SCHEMA, _exists, _filesystem, _commit_text,
                                      _snapshot_at, read_snapshot)


def _classified(rows):
    events = rows.withColumn('event_time', F.expr('''CASE
        WHEN try_cast(timestamp AS bigint) BETWEEN -62135596800000 AND 253402300799999
        THEN timestamp_millis(try_cast(timestamp AS bigint)) END'''))
    return events.withColumn('invalid_reason',
        F.when(F.col('event_time').isNull(), 'invalid_timestamp')
        .when(F.col('visitorid').isNull() | (F.trim('visitorid') == ''), 'missing_visitor')
        .when(F.col('itemid').isNull() | (F.trim('itemid') == ''), 'missing_item')
        .when(F.col('event').isNull() | ~F.col('event').isin('view', 'addtocart', 'transaction'), 'invalid_event'))


def _identities(raw):
    envelope = raw.select(F.col('value').cast('string').alias('payload'), 'topic', 'partition', 'offset')
    records = envelope.select('*', F.from_json('payload', SCHEMA).alias('r')).select('payload','topic','partition','offset','r.*')
    canonical = _classified(records.dropDuplicates(['dataset_version','source_file','source_line']))
    def entity(kind, key):
        return F.struct(F.lit(kind).alias('kind'), key.alias('key'))
    source = F.to_json(F.struct('dataset_version','source_file','source_line'), {'ignoreNullFields':'false'})
    content = F.to_json(F.struct('timestamp','visitorid','event','itemid','transactionid'), {'ignoreNullFields':'false'})
    entities = (canonical.select('payload','topic','partition','offset', F.explode(F.array(
        entity('source',source), entity('content',content),
        F.when(F.col('invalid_reason').isNull(), entity('visitor',F.col('visitorid'))))).alias('e'))
        .select('e.*','payload','topic','partition','offset').filter('kind IS NOT NULL')
        .dropDuplicates(['kind','key']))
    messages = envelope.select(F.lit('message').alias('kind'),
        F.to_json(F.struct('topic','partition','offset')).alias('key'), 'payload','topic','partition','offset')
    return entities.unionByName(messages)


def _initial(run_id):
    return dict(engine='exact-state-v1',run_id=run_id,timezone='UTC',complete=False,
        quality=dict(consumed_count=0,unique_source_count=0,valid_count=0,invalid_count=0,
                     retry_count=0,suspected_duplicate_count=0),
        visitors=0, overview={}, daily=[], invalid_reasons={}, processed_offsets={},samples=[],
        event_time_min=None,event_time_max=None)


def _commit_delta(batch, batch_id, output, run_id):
    spark=batch.sparkSession
    batch.persist()
    try:
        # Exhaust BOTH stateful branches, including replay of an already durable
        # external snapshot. Never advance checkpoint without StateStore versions.
        records = _classified(batch.select('*',F.from_json('payload',SCHEMA).alias('r')).select('*','r.*').drop('r'))
        bad_identity=(F.col('batch_id').isNull() | (F.col('batch_id') != run_id) |
            F.col('source_line').isNull() | (F.col('source_line') < 2) |
            F.col('source_file').isNull() | (F.trim('source_file') == '') |
            F.col('dataset_version').isNull() | (F.trim('dataset_version') == ''))
        groups=(records.withColumn('event_date',F.to_date('event_time')).groupBy(
            'kind','event_date','event','invalid_reason','topic','partition').agg(
                F.count('*').alias('count'),F.max('offset').alias('offset'),
                F.sum(F.when(bad_identity,1).otherwise(0)).alias('bad_identity'),
                F.min('event_time').cast('string').alias('minimum'),
                F.max('event_time').cast('string').alias('maximum')).collect())
        if any(row.bad_identity for row in groups):
            raise ValueError('Missing source provenance or incorrect replay run identity')
        existing=_snapshot_at(spark,output,batch_id)
        if existing:
            if existing['run_id'] != run_id: raise ValueError('Replay run identity mismatch')
            return existing
        snapshot=_snapshot_at(spark,output,batch_id-1) if batch_id else _initial(run_id)
        if snapshot is None:
            raise ValueError('Previous committed streaming result is missing')
        if snapshot['run_id'] != run_id: raise ValueError('Replay run identity mismatch')
        raw_path=output+'/raw/%020d'%batch_id
        if not _exists(spark,raw_path+'/_SUCCESS'):
            fs,path=_filesystem(spark,raw_path)
            if fs.exists(path):fs.delete(path,True)
            batch.filter("kind = 'message'").select(F.col('payload').alias('value'),'topic','partition','offset').coalesce(2).write.parquet(raw_path)
        quality=snapshot['quality']
        counts={kind:0 for kind in ['source','content','visitor','message']}
        daily={(row['event_date'],row['event']):row['count'] for row in snapshot['daily']}
        invalid=snapshot['invalid_reasons'];overview=snapshot['overview']
        for row in groups:
            count=row['count'];counts[row.kind]+=count
            if row.kind=='message':
                offsets=snapshot['processed_offsets'].setdefault(row.topic,{})
                offsets[str(row.partition)]=max(offsets.get(str(row.partition),0),row.offset+1)
            elif row.kind=='source':
                if row.invalid_reason:
                    quality['invalid_count']+=count
                    invalid[row.invalid_reason]=invalid.get(row.invalid_reason,0)+count
                else:
                    quality['valid_count']+=count
                    overview[row.event]=overview.get(row.event,0)+count
                    key=(str(row.event_date),row.event);daily[key]=daily.get(key,0)+count
                    if snapshot['event_time_min'] is None or row.minimum<snapshot['event_time_min']: snapshot['event_time_min']=row.minimum
                    if snapshot['event_time_max'] is None or row.maximum>snapshot['event_time_max']: snapshot['event_time_max']=row.maximum
        quality['consumed_count']+=counts['message']
        quality['unique_source_count']+=counts['source']
        quality['retry_count']=quality['consumed_count']-quality['unique_source_count']
        quality['suspected_duplicate_count']+=counts['source']-counts['content']
        snapshot['visitors']+=counts['visitor']
        snapshot['daily']=[dict(event_date=day,event=event,count=count) for (day,event),count in sorted(daily.items())]
        # Raw monitoring now samples Kafka independently, including unconsumed
        # messages. Keep legacy snapshot fields empty rather than run another
        # distributed order/collect or present stale committed raw samples.
        snapshot.update(microbatch_id=batch_id,updated_at=datetime.now(timezone.utc).isoformat(),
            sample_records=[],samples=[],
            application_id=spark.sparkContext.applicationId,master=spark.sparkContext.master,
            executor_hosts=str(spark.sparkContext._jsc.sc().getExecutorMemoryStatus().keys().toList()))
        _commit_text(spark,output+'/snapshots/%020d'%batch_id,json.dumps(snapshot,ensure_ascii=False))
        return snapshot
    finally:batch.unpersist()


def start_captured_stream(frame, *, output, checkpoint, run_id,trigger_seconds=2,available_now=False):
    spark=frame.sparkSession;spark.conf.set('spark.sql.session.timeZone','UTC')
    if _exists(spark,output+'/raw') and not _exists(spark,checkpoint+'/metadata'):
        raise ValueError('Existing streaming output requires its original checkpoint; refusing silent restart')
    previous=read_snapshot(spark,output)
    if previous and (previous['run_id']!=run_id or previous.get('engine')!='exact-state-v1'):
        raise ValueError('Incompatible streaming output; retain legacy run and start a new replay run')
    spark.conf.set('spark.sql.streaming.stateStore.providerClass','org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider')
    spark.conf.set('spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage','true')
    spark.conf.set('spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB',
                   spark.conf.get('spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB','128'))
    writer=(_identities(frame).writeStream.option('checkpointLocation',checkpoint)
            .foreachBatch(lambda batch,index:_commit_delta(batch,index,output,run_id)))
    return writer.trigger(availableNow=True).start() if available_now else writer.trigger(processingTime=f'{trigger_seconds} seconds').start()
