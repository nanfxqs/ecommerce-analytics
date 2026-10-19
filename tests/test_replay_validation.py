from fastapi.testclient import TestClient
from retailrocket.api import create_app
from test_replay_api import Cluster

def test_reject_unregistered_input_and_invalid_rates(tmp_path):
    with TestClient(create_app(tmp_path/'published',run_store=tmp_path/'runs',cluster=Cluster())) as client:
        assert client.post('/api/replay/runs',json={'dataset':'../full','rate':5}).status_code==422
        for rate in (0,-1,1000001):
            assert client.post('/api/replay/runs',json={'dataset':'small','rate':rate}).status_code==422
        assert client.get('/api/replay/current').json() is None
