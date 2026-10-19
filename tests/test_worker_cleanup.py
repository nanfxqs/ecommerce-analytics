"""Public worker CLI: storage and external-close faults must release every resource."""
import hashlib,json,os,subprocess,sys,fcntl
from pathlib import Path
from retailrocket.ingest import prepare


def test_cursor_persistence_and_external_close_failures_cannot_skip_worker_cleanup(tmp_path):
    source=tmp_path/'source.csv';source.write_text('timestamp,visitorid,event,itemid,transactionid\n0,1,view,a,\n')
    manifest=prepare(source,tmp_path/'input','fixture')
    run_id='v2-'+'c'*32;root=tmp_path/'runs';owned=root/run_id;owned.mkdir(parents=True)
    (owned/'request.json').write_text(json.dumps(dict(run_id=run_id,dataset='small',requested_rate=100)))
    external=tmp_path/'external';(external/'kafka').mkdir(parents=True)
    (external/'kafka/__init__.py').write_text('''from collections import namedtuple
from pathlib import Path
import os
TopicPartition=namedtuple('TopicPartition','topic partition')
root=Path(os.environ['OWNED'])
class KafkaConsumer:
 def __init__(self,**kw):pass
 def partitions_for_topic(self,t):return {0}
 def end_offsets(self,ps):return {p:0 for p in ps}
 def close(self):
  (root/'consumer-closed').touch()
  raise RuntimeError('consumer close fault')
class Future:
 def add_callback(self,callback,*args):callback(*args,None);return self
 def get(self,timeout=None):
  (root/'state.tmp').mkdir(exist_ok=True)
  return None
class KafkaProducer:
 def __init__(self,**kw):pass
 def send(self,*a,**kw):return Future()
 def close(self,**kw):
  (root/'producer-closed').touch()
  raise RuntimeError('producer close fault')
''')
    (external/'kafka/admin.py').write_text('''class NewTopic:
 def __init__(self,*a,**kw):pass
class KafkaAdminClient:
 def __init__(self,**kw):pass
 def create_topics(self,*a,**kw):pass
 def close(self):pass
''')
    code=tmp_path/'code';(code/'scripts').mkdir(parents=True)
    driver=code/'scripts/submit-stream.sh'
    driver.write_text('#!'+sys.executable+'''\nimport sys,time
from pathlib import Path
stop=Path(sys.argv[sys.argv.index('--stop-file')+1])
while not stop.exists():time.sleep(.01)
''');driver.chmod(0o755)
    config=tmp_path/'config.json';config.write_text(json.dumps(dict(root=str(root),code=str(code),brokers='fixture',datasets={'small':dict(manifest=str(manifest),events_sha256=hashlib.sha256((manifest.parent/'events.jsonl').read_bytes()).hexdigest())})))
    env=dict(os.environ,OWNED=str(owned),PYTHONPATH=str(external)+os.pathsep+str(Path(__file__).parents[1]))
    completed=subprocess.run([sys.executable,'-m','retailrocket.edge_worker','work','--config',str(config),'--run-id',run_id],env=env,text=True,capture_output=True,timeout=8)
    stopped=(owned/'stop-stream').exists()
    if not stopped:
        (owned/'stop-stream').touch()
        import time
        time.sleep(.05)
    assert stopped,completed.stderr
    assert (owned/'producer-closed').exists(),completed.stderr
    assert (owned/'consumer-closed').exists(),completed.stderr
    with (owned/'worker.lock').open('a') as lease:fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
    pid=json.loads((owned/'state.json').read_text())['stream_pid']
    try:os.kill(pid,0)
    except ProcessLookupError:pass
    else:raise AssertionError('Owned stream survived failed checkpoint')
