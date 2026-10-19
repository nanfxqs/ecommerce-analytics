"""Approved captured-input pipeline -> durable realtime snapshot -> HTTP seam."""
import json
from pathlib import Path
from fastapi.testclient import TestClient
from retailrocket.api import create_app


def test_live_round_exact_state_survives_restart_and_drives_http(tmp_path):
    from pyspark.sql import SparkSession
    from retailrocket.spark_stream import start_captured_stream, read_snapshot, publish_observation
    spark = (SparkSession.builder.master('local[2]').appName('live-round-contract')
             .config('spark.sql.shuffle.partitions','1')
             .config('spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled','true').getOrCreate())
    inbox=tmp_path/'input'; inbox.mkdir()
    output=str(tmp_path/'output'); checkpoint=str(tmp_path/'checkpoint')
    def row(line,offset,timestamp='0',visitor='1'):
        payload=dict(batch_id='known',dataset_version='v',source_file='events.csv',source_line=line,
                     timestamp=timestamp,visitorid=visitor,itemid='a',event='view',transactionid=None)
        return dict(value=json.dumps(payload),topic='known',partition=0,offset=offset)
    def run(name,rows):
        (inbox/name).write_text(''.join(json.dumps(r)+'\n' for r in rows))
        frame=spark.readStream.schema('value STRING,topic STRING,partition INT,offset LONG').json(str(inbox))
        query=start_captured_stream(frame,output=output,checkpoint=checkpoint,run_id='known',available_now=True)
        query.awaitTermination()
    try:
        # Real filesystem fault AFTER durable result, BEFORE Spark commit log.
        # Recovery must consume state again but reuse that result without adding.
        commits=Path(checkpoint)/'commits'; commits.mkdir(parents=True)
        commits.chmod(0o500)
        import pytest
        try:
            with pytest.raises(Exception,match='(?i)permission denied'):
                run('first.json',[row(2,0,'86400000'),row(3,1),row(2,2,'86400000')])
        finally: commits.chmod(0o700)
        run('first.json',[row(2,0,'86400000'),row(3,1),row(2,2,'86400000')])
        run('second.json',[row(4,3),row(5,4,'bad','invalid-only'),row(2,5,'172800000','ghost')])
        result=read_snapshot(spark,output)
        physical=spark.read.parquet(*[str(p) for p in (Path(output)/'raw').iterdir() if p.is_dir() and (p/'_SUCCESS').exists()]).orderBy('offset').collect()
        assert [r.offset for r in physical]==[0,1,2,3,4,5]
        assert [json.loads(r.value)['source_line'] for r in physical]==[2,3,2,4,5,2]
        assert json.loads(physical[-1].value)['visitorid']=='ghost'
        assert result['quality']==dict(consumed_count=6,unique_source_count=4,valid_count=3,
            invalid_count=1,retry_count=2,suspected_duplicate_count=1)
        assert result['samples']==[]  # raw monitoring is independently sampled from Kafka, not the committed business frame
        assert result.get('sample_records',[])==[]
        assert result['visitors']==1
        assert result['overview']=={'view':3}
        assert result['daily']==[dict(event_date='1970-01-01',event='view',count=2),dict(event_date='1970-01-02',event='view',count=1)]
        assert result['invalid_reasons']=={'invalid_timestamp':1}
        # Captured file input has file-source offsets; at the external Kafka log
        # boundary use a real-filesystem fixture in Spark's durable log format.
        kafka_checkpoint=tmp_path/'kafka-checkpoint'
        (kafka_checkpoint/'offsets').mkdir(parents=True)
        (kafka_checkpoint/'commits').mkdir()
        source=json.dumps({'known':{'0':6,'1':0}})
        (kafka_checkpoint/'offsets/1').write_text('v1\n{}\n'+source+'\n'+source)
        assert publish_observation(spark,output,str(kafka_checkpoint),tmp_path/'observation.json','known') is None
        (kafka_checkpoint/'commits/1').write_text('v1\n{}')
        observed=publish_observation(spark,output,str(kafka_checkpoint),tmp_path/'observation.json','known')
        assert observed['committed']=={'0':6,'1':0}
        assert observed['result_offsets']=={'0':6,'1':0}
        class Cluster:
            def datasets(self): return [{'id':'small','label':'固定样本','rows':6}]
            def start(self,run): return {'phase':'running','confirmed':6}
            def observe(self,run): return dict(observed,phase='running',confirmed=6,rate=5,ends={'0':6,'1':0})
        with TestClient(create_app(tmp_path/'published',run_store=tmp_path/'runs',cluster=Cluster(),poll_seconds=.01)) as client:
            response=client.post('/api/replay/runs',json={'dataset':'small','rate':5})
            assert response.status_code==201
            import time
            for _ in range(100):
                state=client.get('/api/replay/current').json()
                if state.get('snapshot'): break
                time.sleep(.01)
            assert state['snapshot']['quality']['valid_count']==3
            assert state['snapshot']['visitors']==1
            assert state['lag']==0
    finally:spark.stop()
