"""HTTP -> public edge observation CLI: completion during broker I/O is authoritative."""
import json,os,subprocess,sys,time
from pathlib import Path
from fastapi.testclient import TestClient
from retailrocket.api import create_app


def test_normal_worker_completion_during_kafka_lookup_does_not_become_false_failure(tmp_path):
    external=tmp_path/'external';(external/'kafka').mkdir(parents=True)
    (external/'kafka/__init__.py').write_text('''from collections import namedtuple
from pathlib import Path
import os,json
TopicPartition=namedtuple('TopicPartition','topic partition')
class KafkaConsumer:
 def __init__(self,**kw):pass
 def partitions_for_topic(self,t):return {0}
 def end_offsets(self,parts):
  path=Path(os.environ['OWNED'])/'state.json';state=json.loads(path.read_text())
  state.update(phase='stopped',complete=False,early_stopped=True,stream_released=True,final={'0':0});state.pop('error',None)
  path.write_text(json.dumps(state))
  return {p:0 for p in parts}
 def close(self):pass
''')
    root=tmp_path/'edge';root.mkdir()
    config=tmp_path/'config.json';config.write_text(json.dumps(dict(root=str(root),brokers='fixture')))
    class Cluster:
        def datasets(self):return [dict(id='small',rows=1,label='fixture')]
        def start(self,run):
            self.owned=root/run['run_id'];self.owned.mkdir()
            (self.owned/'state.json').write_text(json.dumps(dict(run_id=run['run_id'],topic=run['run_id'],phase='draining',confirmed=0,starting={'0':0})))
            return dict(phase='draining',error='worker 已中断')
        def observe(self,run):
            env=dict(os.environ,OWNED=str(self.owned),PYTHONPATH=str(external)+os.pathsep+str(Path(__file__).parents[1]))
            response=subprocess.run([sys.executable,'-m','retailrocket.edge_worker','observe','--config',str(config),'--run-id',run['run_id']],env=env,capture_output=True,text=True,timeout=3)
            assert response.returncode==0,response.stderr
            return json.loads(response.stdout)
    with TestClient(create_app(tmp_path/'pub',run_store=tmp_path/'runs',cluster=Cluster(),poll_seconds=.01)) as client:
        client.post('/api/replay/runs',json=dict(dataset='small',rate=5))
        for _ in range(100):
            state=client.get('/api/replay/current').json()
            if state['phase'] in ('stopped','failed'):break
            time.sleep(.01)
        assert state['phase']=='stopped',state
        assert not state.get('error'),state
