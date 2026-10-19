"""Public HTTP application lifecycle retains exclusivity through slow external fetch."""
import threading,time
import pytest
from fastapi.testclient import TestClient
from retailrocket.api import create_app


def test_shutdown_keeps_run_lease_until_inflight_external_fetch_really_exits(tmp_path):
    class Cluster:
        entered=threading.Event();release=threading.Event()
        def datasets(self):return [dict(id='small',rows=1,label='fixture')]
        def start(self,run):return dict(phase='running')
        def observe(self,run):return dict(phase='completed',complete=True,stream_released=True,ends={'0':1},committed={'0':1},result_offsets={'0':1})
        def finalize(self,run):return dict(historical_status='exported')
        def fetch(self,run,destination):
            self.entered.set();self.release.wait(90)
            raise RuntimeError('external fetch released')
    cluster=Cluster();root=tmp_path/'runs';store=tmp_path/'pub'
    client=TestClient(create_app(store,run_store=root,cluster=cluster,poll_seconds=.01));client.__enter__()
    closed=threading.Event()
    def close():
        client.__exit__(None,None,None);closed.set()
    try:
        client.post('/api/replay/runs',json=dict(dataset='small',rate=5))
        assert cluster.entered.wait(2)
        thread=threading.Thread(target=close);thread.start()
        # Reproduces the former 45-second lease release while fetch still runs.
        assert not closed.wait(45.3),'Application released its lease with a live supervisor'
        with pytest.raises(BlockingIOError):create_app(store,run_store=root,cluster=cluster)
    finally:
        cluster.release.set()
        if 'thread' in locals():thread.join(3)
        else:client.__exit__(None,None,None)
    assert closed.is_set()
