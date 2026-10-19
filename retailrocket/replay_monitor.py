"""Host observation metadata, separate from durable source and processing progress."""
from datetime import datetime, timezone
import json
import time


class Monitor:
    def __init__(self):
        self.previous=None

    def observe(self,current,update):
        result=dict(current,**update)
        if update.get('phase') in ('completed','stopped') and not update.get('error'):
            result['error']=None
        result['source_cursor']=result.get('cursor',result.get('confirmed',0))
        ends=result.get('ends',{});starting=result.get('starting',{})
        if any(end<starting.get(partition,0) for partition,end in ends.items()):
            raise ValueError('Kafka end precedes original run start')
        result['physical_messages']=sum(end-starting.get(partition,0) for partition,end in ends.items())
        now=time.monotonic();session=result.get('ack_session');count=result.get('producer_acknowledged')
        rate=0.
        if self.previous and session is not None and type(count) is int:
            previous_run,previous_session,previous_count,previous_time=self.previous
            if result['run_id']==previous_run and session==previous_session and count>=previous_count and now>previous_time:
                rate=(count-previous_count)/(now-previous_time)
        if session is not None and type(count) is int:
            self.previous=(result['run_id'],session,count,now)
        result['rate']=rate if result['phase']=='running' else 0.
        result['rate_definition']='producer_acknowledgements_per_observation_second'
        snapshot=result.get('snapshot')
        if snapshot is not None:
            identity=json.dumps([snapshot.get('run_id'),snapshot.get('microbatch_id'),snapshot.get('processed_offsets')],sort_keys=True)
            received=current.get('result_received_at') if current.get('result_identity')==identity else None
            received=received or datetime.now(timezone.utc).isoformat()
            result.update(result_identity=identity,result_received_at=received,snapshot=dict(snapshot,result_received_at=received))
        return result
