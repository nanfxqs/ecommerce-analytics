import time
from fastapi.testclient import TestClient
from retailrocket.api import create_app
from test_replay_api import Cluster

class ControlledCluster(Cluster):
    phase='running'
    def control(self,run,action):
        self.last_control=dict(run['pending_control'],acknowledged=True)
        self.phase={'pause':'paused','continue':'running','stop':'stopped'}[action]
        return {'accepted':True,'action':action,'operation_id':'operation-'+action}
    def observe(self,run):
        result=super().observe(run); result['phase']=self.phase
        result['last_control']=getattr(self,'last_control',None)
        return result

def wait(client,phase):
    for _ in range(100):
        state=client.get('/api/replay/current').json()
        if state['phase']==phase:return state
        time.sleep(.01)
    raise AssertionError(state)

def test_pause_continue_and_early_stop_preserve_same_round(tmp_path):
    with TestClient(create_app(tmp_path/'published',run_store=tmp_path/'runs',cluster=ControlledCluster(),poll_seconds=.01)) as client:
        run=client.post('/api/replay/runs',json={'dataset':'small','rate':5}).json()
        paused=client.post('/api/replay/current/pause')
        assert paused.status_code==202
        state=wait(client,'paused'); assert state['run_id']==run['run_id']
        assert client.post('/api/replay/current/pause').status_code==200
        assert client.post('/api/replay/current/continue').status_code==202
        wait(client,'running')
        assert client.post('/api/replay/current/stop').status_code==202
        stopped=wait(client,'stopped')
        assert stopped['run_id']==run['run_id'] and stopped['complete'] is False
        assert client.post('/api/replay/current/stop').status_code==200
        assert client.post('/api/replay/current/continue').status_code==409

def test_old_cluster_observation_cannot_undo_acknowledged_control(tmp_path):
    from threading import Event
    class DelayedCluster(ControlledCluster):
        entered=Event(); release=Event(); returned=Event()
        def observe(self,run):
            if not self.entered.is_set():
                old=super().observe(run)
                self.entered.set(); self.release.wait(3); self.returned.set()
                return old
            return super().observe(run)
        def control(self,run,action):
            result=super().control(run,action)
            return dict(result,phase=self.phase,last_control=self.last_control,confirmed=1,cursor=1,rate=5)
    cluster=DelayedCluster()
    with TestClient(create_app(tmp_path/'published',run_store=tmp_path/'runs',cluster=cluster,poll_seconds=.02)) as client:
        client.post('/api/replay/runs',json={'dataset':'small','rate':5})
        assert cluster.entered.wait(1)
        assert client.post('/api/replay/current/pause').status_code==202
        assert client.get('/api/replay/current').json()['phase']=='paused'
        cluster.release.set(); assert cluster.returned.wait(1)
        time.sleep(.05)
        assert client.get('/api/replay/current').json()['phase']=='paused'
