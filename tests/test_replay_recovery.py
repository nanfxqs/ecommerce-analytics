import time
from fastapi.testclient import TestClient
from retailrocket.api import create_app
from test_replay_api import Cluster

class Recoverable(Cluster):
    phase='running'
    def quiesce(self,run):
        self.phase='paused'
        return dict(self.observe(run),phase='paused')
    def recover(self,run):
        self.phase='running'
        return dict(phase='running',cursor=2,confirmed=2)
    def observe(self,run): return dict(super().observe(run),phase=self.phase)

def test_restart_waits_for_manual_same_round_recovery(tmp_path):
    cluster=Recoverable()
    def app():return create_app(tmp_path/'published',run_store=tmp_path/'runs',cluster=cluster,poll_seconds=.01)
    with TestClient(app()) as client:
        identity=client.post('/api/replay/runs',json={'dataset':'small','rate':5}).json()['run_id']
    with TestClient(app()) as client:
        time.sleep(.05)
        state=client.get('/api/replay/current').json()
        assert state['phase']=='recovery_required'
        assert state['remote_phase']=='paused'
        assert client.post('/api/replay/runs',json={'dataset':'small','rate':5}).status_code==409
        assert client.post('/api/replay/current/continue').status_code==409
        response=client.post('/api/replay/current/recover')
        assert response.status_code==202
        assert client.get('/api/replay/current').json()['run_id']==identity
        assert client.get('/api/replay/current').json()['phase']=='running'

def test_corrupt_checkpoint_stays_same_round_and_reports_error(tmp_path):
    class Damaged(Recoverable):
        def recover(self,run):raise ValueError('Original checkpoint missing or corrupt; refusing reset')
    cluster=Damaged()
    def app():return create_app(tmp_path/'published',run_store=tmp_path/'runs',cluster=cluster,poll_seconds=.01)
    with TestClient(app()) as client:
        identity=client.post('/api/replay/runs',json={'dataset':'small','rate':5}).json()['run_id']
    with TestClient(app()) as client:
        assert client.post('/api/replay/current/recover').status_code==503
        state=client.get('/api/replay/current').json()
        assert state['phase']=='recovery_required' and state['run_id']==identity
        assert 'refusing reset' in state['recovery_error']
        assert client.post('/api/replay/runs',json={'dataset':'small','rate':5}).status_code==409
