from fastapi.testclient import TestClient
from retailrocket.api import create_app
from retailrocket.publication import publish_batch, current_snapshot
from retailrocket.validation import validate_analysis
from test_publication_cli import complete_export
from test_auto_publication import Cluster, wait


def test_live_publication_does_not_replace_pinned_history(tmp_path):
    replay, export = complete_export(tmp_path, 'fixed-history')
    fixed = tmp_path / 'fixed/published'
    output = tmp_path / 'live/published'
    publish_batch(fixed, replay, export, validate=validate_analysis)
    pointer = (fixed / 'current.json').read_bytes()
    with TestClient(create_app(fixed, publication_store=output,
                              cluster=Cluster(tmp_path), poll_seconds=.01)) as client:
        assert client.get('/api/replay/datasets').status_code == 200
        run = client.post('/api/replay/runs', json={'dataset': 'small', 'rate': 5})
        assert run.status_code == 201
        state = wait(client, lambda s: s.get('historical_status') == 'published')
        assert current_snapshot(output).batch_id == state['run_id']
        history = client.get('/api/history/overview').json()
        assert history['snapshot']['batch_id'] == 'fixed-history'
        assert history['snapshot']['pinned'] is True
        assert (fixed / 'current.json').read_bytes() == pointer
