"""Public worker CLI seam with external Kafka/process fixtures, real durable files."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import pytest
from retailrocket.ingest import prepare

@pytest.mark.parametrize(('flush_records','seconds','delay','expected'),[(1,1,0,34),(16,1,0,32),(1000,.02,.08,34)])
def test_worker_crash_preserves_bounded_contiguous_ack_prefix(tmp_path,flush_records,seconds,delay,expected):
    source=tmp_path/'source.csv'
    source.write_text('timestamp,visitorid,event,itemid,transactionid\n'+''.join(f'{i},1,view,a,\n' for i in range(61)))
    manifest=prepare(source,tmp_path/'prepared','fixture')
    import hashlib
    events=manifest.parent/'events.jsonl'
    fake=tmp_path/'external';(fake/'kafka').mkdir(parents=True)
    (fake/'kafka/__init__.py').write_text('''from collections import namedtuple
import os,time
TopicPartition=namedtuple('TopicPartition','topic partition')
class KafkaConsumer:
 def __init__(self,**kw):pass
 def partitions_for_topic(self,t):return {0,1,2}
 def end_offsets(self,ps):return {p:0 for p in ps}
 def close(self):pass
class Future:
 def add_callback(self,f,*args):
  try:f(*args,type('Metadata',(),dict(topic='fixture',partition=0,offset=1))())
  except Exception:pass
  return self
 def get(self,timeout=None):time.sleep(float(os.environ.get('ACK_DELAY','0')));return None
class KafkaProducer:
 def __init__(self,**kw):pass
 def send(self,*a,**kw):return Future()
 def close(self,**kw):pass
''')
    (fake/'kafka/admin.py').write_text('''class NewTopic:
 def __init__(self,*a,**kw):pass
class KafkaAdminClient:
 def __init__(self,**kw):pass
 def create_topics(self,*a,**kw):pass
 def close(self):pass
''')
    code=tmp_path/'driver';(code/'scripts').mkdir(parents=True)
    driver=code/'scripts/submit-stream.sh';driver.write_text('#!/bin/sh\nprintf %s \"$RETAILROCKET_SHUFFLE_PARTITIONS\" > \"$0.partitions\"\nprintf \"%s\\n\" \"$@\" > \"$0.args\"\nprintf %s \"$RETAILROCKET_STREAM_ROCKSDB_CHANGELOG\" > \"$0.changelog\"\nexec sleep 120\n');driver.chmod(0o755)
    run_id='v2-'+'1'*32;root=tmp_path/'runs';owned=root/run_id;owned.mkdir(parents=True)
    (owned/'request.json').write_text(json.dumps(dict(run_id=run_id,dataset='small',requested_rate=1000000)))
    config=dict(root=str(root),code=str(code),brokers='external:9092',audit_crash_after_ack=35,
                cursor_flush_records=flush_records,cursor_flush_seconds=seconds,shuffle_partitions=24,audit_ack_log=True,observation_interval_seconds=.1,rocksdb_changelog=True,
                datasets={'small':dict(manifest=str(manifest),events_sha256=hashlib.sha256(events.read_bytes()).hexdigest())})
    path=tmp_path/'config.json';path.write_text(json.dumps(config))
    env=dict(os.environ,ACK_DELAY=str(delay),PYTHONPATH=str(fake)+os.pathsep+str(Path(__file__).parents[1]))
    result=subprocess.run([sys.executable,'-m','retailrocket.edge_worker','work','--config',str(path),'--run-id',run_id],env=env,capture_output=True,text=True,timeout=30)
    state=json.loads((owned/'state.json').read_text())
    try:
        assert result.returncode==86,result.stderr
        traced=[json.loads(line) for line in (owned/'acknowledgements.jsonl').read_text().splitlines()]
        assert len(traced)==61
        assert traced[0]['source_line']==2 and traced[0]['topic']=='fixture'
        assert traced[0]['ack_monotonic_ns']>0 and traced[0]['ack_wall_ns']>0
        assert len(traced[0]['source_record_id'])==64
        assert Path(str(driver)+'.partitions').read_text()=='24'
        assert '--observation-interval-seconds\n0.1' in Path(str(driver)+'.args').read_text()
        assert state['stream_profile']['shuffle_partitions']==24
        assert state['stream_profile']['rocksdb_changelog'] is True
        assert Path(str(driver)+'.changelog').read_text()=='true'
        assert state['cursor']==expected
        assert state['confirmed']==expected
        assert 35-state['cursor']<=flush_records
    finally:
        os.killpg(state['stream_pid'],signal.SIGTERM)
    config['rocksdb_changelog']=False;path.write_text(json.dumps(config))
    rejected=subprocess.run([sys.executable,'-m','retailrocket.edge_worker','work','--resume','--config',str(path),'--run-id',run_id],env=env,capture_output=True,text=True,timeout=10)
    unchanged=json.loads((owned/'state.json').read_text())
    assert rejected.returncode==0 and unchanged['phase']=='failed'
    assert unchanged['stream_profile']['rocksdb_changelog'] is True
    assert unchanged['cursor']==expected
    config['rocksdb_changelog']=True
    config['shuffle_partitions']=2;path.write_text(json.dumps(config))
    recovered=subprocess.run([sys.executable,'-m','retailrocket.edge_worker','work','--resume','--config',str(path),'--run-id',run_id],env=env,capture_output=True,text=True,timeout=10)
    final=json.loads((owned/'state.json').read_text())
    assert recovered.returncode==0,recovered.stderr
    assert final['phase']=='failed'
    assert 'checkpoint state partitions cannot migrate' in final['error']
    assert final['cursor']==expected
    assert final['stream_profile']['shuffle_partitions']==24
