"""Public observe CLI reads Kafka tails while committed business output is frozen."""
import json,os,subprocess,sys
from pathlib import Path


def test_kafka_tail_samples_advance_independently_of_frozen_committed_result(tmp_path):
    run_id='v2-'+'d'*32;root=tmp_path/'runs';owned=root/run_id;owned.mkdir(parents=True)
    state=dict(run_id=run_id,topic=run_id,phase='completed',confirmed=1,cursor=1,starting={'0':0,'1':0,'2':0})
    (owned/'state.json').write_text(json.dumps(state))
    observed=dict(snapshot=dict(run_id=run_id,microbatch_id=0,updated_at='old',processed_offsets={run_id:{'0':1}},samples=[dict(offset=0)]),committed={'0':1,'1':0,'2':0},result_offsets={'0':1,'1':0,'2':0},samples=[dict(offset=0)])
    (owned/'stream-observation.json').write_text(json.dumps(observed))
    broker=tmp_path/'broker.json';broker.write_text(json.dumps(dict(end=5)))
    external=tmp_path/'external';(external/'kafka').mkdir(parents=True)
    (external/'kafka/__init__.py').write_text('''from collections import namedtuple
from pathlib import Path
import json,os
TopicPartition=namedtuple('TopicPartition','topic partition')
Record=namedtuple('Record','topic partition offset timestamp value')
class KafkaConsumer:
 def __init__(self,**kwargs):self.positions={}
 def partitions_for_topic(self,t):return {0,1,2}
 def end_offsets(self,parts):return {p:json.loads(Path(os.environ['BROKER']).read_text())['end'] if p.partition==0 else 0 for p in parts}
 def assign(self,parts):self.parts=parts
 def seek(self,p,offset):self.positions[p]=offset
 def poll(self,timeout_ms=0,max_records=None):
  assert 0<=timeout_ms<=200
  if json.loads(Path(os.environ['BROKER']).read_text()).get('fail'):raise RuntimeError('raw fetch unavailable')
  end=json.loads(Path(os.environ['BROKER']).read_text())['end']; result={}
  for p in self.parts:
   if p.partition==0:
    result[p]=[Record(p.topic,0,i,1000+i,json.dumps({'source_line':i+2,'event':'view'}).encode()) for i in range(self.positions[p],end)]
    self.positions[p]=end
  return result
 def close(self):pass
''')
    config=tmp_path/'config.json';config.write_text(json.dumps(dict(root=str(root),brokers='fixture')))
    env=dict(os.environ,BROKER=str(broker),PYTHONPATH=str(external)+os.pathsep+str(Path(__file__).parents[1]))
    def observe():
        result=subprocess.run([sys.executable,'-m','retailrocket.edge_worker','observe','--config',str(config),'--run-id',run_id],env=env,text=True,capture_output=True,timeout=3)
        assert result.returncode==0,result.stderr
        return json.loads(result.stdout)
    first=observe()
    assert first['raw_samples'][0]['offset']==4
    broker.write_text(json.dumps(dict(end=30)))
    later=observe()
    assert len(later['raw_samples'])==20
    assert later['raw_samples'][0]['offset']==29
    assert later['raw_samples'][0]['record']['source_line']==31
    assert later['raw_samples'][0]['topic']==run_id
    assert later['ends']['0']==30
    assert later['committed']['0']==1 and later['result_offsets']['0']==1
    assert later['snapshot']==first['snapshot']
    broker.write_text(json.dumps(dict(end=31,fail=True)))
    failed=observe()
    assert failed['raw_samples']==[] and 'raw fetch unavailable' in failed['raw_sample_error']
    assert failed['ends']['0']==31 and failed['committed']['0']==1
    assert failed['snapshot']==first['snapshot']
