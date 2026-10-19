"""Approved HTTP observation seam: raw counters, ACK rates and snapshot freshness."""
import time
from fastapi.testclient import TestClient
from retailrocket.api import create_app


def wait(client,predicate):
    for _ in range(100):
        state=client.get('/api/replay/current').json()
        if predicate(state):return state
        time.sleep(.01)
    raise AssertionError(state)


class Cluster:
    count=60;phase='running';batch=1
    def datasets(self):return [dict(id='small',rows=61,label='fixture')]
    def start(self,run):return dict(phase='running')
    def observe(self,run):return dict(phase=self.phase,cursor=61,confirmed=61,starting={'0':0},ends={'0':62},committed={'0':50},result_offsets={'0':50},rate=999,producer_acknowledged=self.count,ack_session='producer-one')


def test_physical_kafka_messages_source_cursor_and_observed_ack_rate_are_distinct(tmp_path):
    cluster=Cluster()
    with TestClient(create_app(tmp_path/'pub',run_store=tmp_path/'runs',cluster=cluster,poll_seconds=.02)) as client:
        client.post('/api/replay/runs',json=dict(dataset='small',rate=5))
        state=wait(client,lambda s:s.get('physical_messages')==62)
        assert state['source_cursor']==61 and state['confirmed']==61
        cluster.count=62
        state=wait(client,lambda s:s.get('rate',0)>0)
        assert state['rate_definition']=='producer_acknowledgements_per_observation_second'
        cluster.phase='paused'
        state=wait(client,lambda s:s['phase']=='paused')
        assert state['rate']==0
        cluster.phase='completed'
        state=wait(client,lambda s:s['phase']=='completed')
        assert state['rate']==0 and state['physical_messages']==62


def test_first_host_receipt_is_pinned_to_result_identity_not_poll_time(tmp_path):
    class Results(Cluster):
        def observe(self,run):
            return dict(super().observe(run),snapshot=dict(run_id=run['run_id'],microbatch_id=self.batch,
                updated_at='2026-10-19T01:00:00+00:00',processed_offsets={run['run_id']:{'0':self.batch}},overview={'view':1}))
    cluster=Results()
    with TestClient(create_app(tmp_path/'pub',run_store=tmp_path/'runs',cluster=cluster,poll_seconds=.02)) as client:
        client.post('/api/replay/runs',json=dict(dataset='small',rate=5))
        first=wait(client,lambda s:s.get('result_received_at'))
        received=first['result_received_at']
        time.sleep(.08)
        old=client.get('/api/replay/current').json()
        assert old['updated_at']!=first['updated_at']
        assert old['result_received_at']==received
        assert old['snapshot']['result_received_at']==received
        assert old['snapshot']['updated_at']=='2026-10-19T01:00:00+00:00'
        cluster.batch=2
        newer=wait(client,lambda s:s.get('result_received_at')!=received)
        assert newer['snapshot']['result_received_at']==newer['result_received_at']
