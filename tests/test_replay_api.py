from fastapi.testclient import TestClient
from retailrocket.api import create_app

class Cluster:
    def datasets(self): return [{'id':'small','label':'固定小样本','rows':3}]
    def start(self, run): return {'phase':'running','confirmed':0}
    def observe(self, run):
        return dict(phase='running',confirmed=3,rate=5,ends={'0':3},committed={'0':1},result_offsets={'0':2},samples=[{'event':'view'}],snapshot=None)

def test_start_single_round_and_observe_durable_progress(tmp_path):
    app=create_app(tmp_path/'published',run_store=tmp_path/'runs',cluster=Cluster(),poll_seconds=0.02)
    import time
    with TestClient(app) as client:
        assert client.get('/api/replay/datasets').json()[0]['rows']==3
        response=client.post('/api/replay/runs',json={'dataset':'small','rate':5})
        assert response.status_code==201
        identity=response.json()['run_id']
        assert client.post('/api/replay/runs',json={'dataset':'small','rate':5}).status_code==409
        for _ in range(100):
            state=client.get('/api/replay/current').json()
            if state.get('confirmed')==3: break
            time.sleep(.02)
        assert state['run_id']==identity
        assert state['lag']==2
        assert state['result_offsets']=={'0':2}
        assert state['committed']=={'0':1}
        assert state['complete'] is False
    with TestClient(create_app(tmp_path/'published',run_store=tmp_path/'runs',cluster=Cluster())) as client:
        assert client.get('/api/replay/current').json()['run_id']==identity
        assert client.post('/api/replay/runs',json={'dataset':'small','rate':5}).status_code==409
