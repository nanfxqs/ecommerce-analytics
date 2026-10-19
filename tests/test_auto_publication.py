"""Approved HTTP + real publication seam, with the remote cluster injected."""
import time
from fastapi.testclient import TestClient
from retailrocket.api import create_app
from retailrocket.publication import publish_batch,current_snapshot
from retailrocket.validation import validate_analysis
from test_publication_cli import complete_export


class Cluster:
    def __init__(self,tmp_path,*,stopped=False): self.tmp=tmp_path; self.calls=0; self.stopped=stopped; self.payload=None
    def datasets(self): return [dict(id='small',label='已知样本',rows=2)]
    def start(self,run): return dict(phase='running')
    def observe(self,run): return dict(phase='stopped' if self.stopped else 'completed',complete=not self.stopped,early_stopped=self.stopped,stream_released=True,confirmed=2,ends={'0':2},committed={'0':2},result_offsets={'0':2})
    def finalize(self,run):
        self.calls+=1
        return dict(historical_status='exported',historical_application_id='application_known_batch')
    def fetch(self,run,destination):
        if self.payload is None: self.payload=complete_export(self.tmp,run['run_id'])
        return self.payload


def wait(client,predicate):
    for _ in range(200):
        state=client.get('/api/replay/current').json()
        if predicate(state): return state
        time.sleep(.01)
    raise AssertionError(state)


def test_normal_stream_completion_automatically_publishes_and_retains_one_round_gate(tmp_path):
    cluster=Cluster(tmp_path)
    with TestClient(create_app(tmp_path/'published',run_store=tmp_path/'runs',cluster=cluster,poll_seconds=.01)) as client:
        run=client.post('/api/replay/runs',json=dict(dataset='small',rate=5)).json()
        state=wait(client,lambda s:s.get('historical_status')=='published')
        assert state['run_id']==run['run_id'] and state['phase']=='completed'
        assert state['published_batch_id']==run['run_id']
        history=client.get('/api/history/overview').json()
        assert history['snapshot']['batch_id']==run['run_id']
        assert history['data']['counts']['view']==2
        assert history['data']['visitors']['visitors']==1
        assert cluster.calls==1


def test_history_failure_keeps_old_snapshot_and_explicit_retry_is_idempotent(tmp_path):
    old_replay,old_export=complete_export(tmp_path,'previous')
    store=tmp_path/'published'; publish_batch(store,old_replay,old_export,validate=validate_analysis)
    old=current_snapshot(store).directory
    failures=[True]
    def checkpoint(snapshot):
        if failures[0]: raise RuntimeError('intentional failure before pointer switch')
    cluster=Cluster(tmp_path)
    with TestClient(create_app(store,run_store=tmp_path/'runs',cluster=cluster,poll_seconds=.01,publication_checkpoint=checkpoint)) as client:
        run=client.post('/api/replay/runs',json=dict(dataset='small',rate=5)).json()
        failed=wait(client,lambda s:s.get('failure_stage')=='history')
        assert failed['phase']=='failed' and 'before pointer' in failed['error']
        assert current_snapshot(store).directory==old
        assert client.post('/api/replay/runs',json=dict(dataset='small',rate=5)).status_code==409
        failures[0]=False
        assert client.post('/api/replay/current/retry-history').status_code==202
        done=wait(client,lambda s:s.get('historical_status')=='published')
        assert done['run_id']==run['run_id']
        assert client.get('/api/history/overview').json()['data']['counts']['view']==2
        assert len(list((store/'batches'/run['run_id']).iterdir()))==1
        assert client.post('/api/replay/current/retry-history').status_code==409


def test_early_stop_never_finalizes_or_publishes_and_history_stages_block_new_run(tmp_path):
    cluster=Cluster(tmp_path,stopped=True)
    with TestClient(create_app(tmp_path/'pub',run_store=tmp_path/'runs',cluster=cluster,poll_seconds=.01)) as client:
        client.post('/api/replay/runs',json=dict(dataset='small',rate=5))
        wait(client,lambda s:s['phase']=='stopped')
        assert cluster.calls==0
        assert client.get('/api/history/overview').json()['snapshot'] is None
    class Slow(Cluster):
        def finalize(self,run): return dict(historical_status='analyzing')
    with TestClient(create_app(tmp_path/'pub',run_store=tmp_path/'other-runs',cluster=Slow(tmp_path),poll_seconds=.01)) as client:
        client.post('/api/replay/runs',json=dict(dataset='small',rate=5))
        wait(client,lambda s:s['phase']=='analyzing')
        assert client.post('/api/replay/runs',json=dict(dataset='small',rate=5)).status_code==409


def test_unreleased_stream_cannot_enter_batch_or_free_the_single_run_slot(tmp_path):
    class Unreleased(Cluster):
        def observe(self,run): return dict(super().observe(run),stream_released=False)
    cluster=Unreleased(tmp_path)
    with TestClient(create_app(tmp_path/'pub',run_store=tmp_path/'runs',cluster=cluster,poll_seconds=.01)) as client:
        client.post('/api/replay/runs',json=dict(dataset='small',rate=5))
        state=wait(client,lambda s:s.get('observation_error'))
        assert state['phase']!='completed'
        assert client.post('/api/replay/runs',json=dict(dataset='small',rate=5)).status_code==409
        assert cluster.calls==0


def test_mismatched_batch_artifact_cannot_change_current_publication(tmp_path):
    class Wrong(Cluster):
        def fetch(self,run,destination): return complete_export(self.tmp,'wrong-batch')
    cluster=Wrong(tmp_path)
    with TestClient(create_app(tmp_path/'pub',run_store=tmp_path/'runs',cluster=cluster,poll_seconds=.01)) as client:
        client.post('/api/replay/runs',json=dict(dataset='small',rate=5))
        state=wait(client,lambda s:s.get('failure_stage')=='history')
        assert '身份' in state['error']
        assert client.get('/api/history/overview').json()['snapshot'] is None


def test_failed_remote_analysis_requires_explicit_retry_and_preserves_retry_across_lease_handoff(tmp_path):
    class Retry(Cluster):
        def __init__(self,tmp): super().__init__(tmp); self.retried=[]
        def finalize(self,run):
            self.calls+=1
            if self.calls==1: return dict(historical_status='failed',historical_error='batch failed')
            self.retried.append(run.get('history_retry'))
            return dict(historical_status='pending' if self.calls==2 else 'exported')
    cluster=Retry(tmp_path)
    with TestClient(create_app(tmp_path/'pub',run_store=tmp_path/'runs',cluster=cluster,poll_seconds=.01)) as client:
        client.post('/api/replay/runs',json=dict(dataset='small',rate=5))
        wait(client,lambda s:s.get('failure_stage')=='history')
        time.sleep(.03)
        assert cluster.calls==1
        assert client.get('/api/history/overview').json()['snapshot'] is None
        client.post('/api/replay/current/retry-history')
        wait(client,lambda s:s.get('historical_status')=='published')
        assert cluster.retried==[True,True]


def test_restart_waits_for_manual_recovery_then_adopts_existing_historical_stage(tmp_path):
    class Recoverable(Cluster):
        finish=False
        def quiesce(self,run): return self.observe(run)
        def recover(self,run): return self.observe(run)
        def finalize(self,run):
            return dict(historical_status='exported' if self.finish else 'analyzing')
    cluster=Recoverable(tmp_path); store=tmp_path/'pub'; runs=tmp_path/'runs'
    with TestClient(create_app(store,run_store=runs,cluster=cluster,poll_seconds=.01)) as client:
        run=client.post('/api/replay/runs',json=dict(dataset='small',rate=5)).json()
        wait(client,lambda s:s['phase']=='analyzing')
    cluster.finish=True
    with TestClient(create_app(store,run_store=runs,cluster=cluster,poll_seconds=.01)) as client:
        wait(client,lambda s:s['phase']=='recovery_required')
        time.sleep(.03)
        assert client.get('/api/history/overview').json()['snapshot'] is None
        assert client.post('/api/replay/current/recover').status_code==202
        state=wait(client,lambda s:s.get('historical_status')=='published')
        assert state['run_id']==run['run_id']
