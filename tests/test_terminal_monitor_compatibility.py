"""Public HTTP restart preserves legacy terminal results while restoring monitor counts."""
import json
import pytest
from fastapi.testclient import TestClient
from retailrocket.api import create_app


@pytest.mark.parametrize('with_snapshot',[False,True])
def test_restart_derives_legacy_terminal_counts_without_refreshing_result_or_publication(tmp_path,with_snapshot):
    run_id='v2-'+'e'*32;root=tmp_path/'runs';root.mkdir()
    snapshot=dict(run_id=run_id,microbatch_id=7,updated_at='2026-10-19T01:00:00+00:00',result_received_at='2026-10-19T09:00:00+00:00',quality=dict(consumed_count=16,unique_source_count=14,retry_count=2)) if with_snapshot else None
    legacy=dict(run_id=run_id,phase='completed',confirmed=14,ends={'0':12,'1':7},starting={'0':2,'1':1},rate=999,
                historical_status='published',published_batch_id=run_id,published_revision='a'*64,
                result_received_at='2026-10-19T09:00:00+00:00',result_identity='original-result',snapshot=snapshot)
    (root/'current.json').write_text(json.dumps(legacy))
    class Offline:
        def observe(self,run):raise AssertionError('Terminal restoration must not contact the cluster')
    for _ in range(2):
        with TestClient(create_app(tmp_path/'pub',run_store=root,cluster=Offline(),poll_seconds=.01)) as client:
            current=client.get('/api/replay/current').json()
            assert current['source_cursor']==14
            assert current['physical_messages']==16
            assert current['rate']==0
            for key in ('snapshot','published_batch_id','published_revision','historical_status','result_received_at','result_identity'):
                assert current[key]==legacy[key]


def test_legacy_terminal_missing_counter_evidence_is_unknown_not_zero(tmp_path):
    root=tmp_path/'runs';root.mkdir()
    (root/'current.json').write_text(json.dumps(dict(run_id='v2-'+'f'*32,phase='stopped',rate=500)))
    class Offline:
        def observe(self,run):raise AssertionError('No remote observation for terminal run')
    with TestClient(create_app(tmp_path/'pub',run_store=root,cluster=Offline(),poll_seconds=.01)) as client:
        state=client.get('/api/replay/current').json()
        assert state['source_cursor'] is None
        assert state['physical_messages'] is None
        assert state['rate']==0
