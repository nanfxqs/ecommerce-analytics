#!/usr/bin/env python3
"""Observe the real HTTP→edge→Kafka→YARN→published V2 full acceptance chain."""
import argparse
from datetime import datetime,timezone
import json
from pathlib import Path
import time
import urllib.request
p=argparse.ArgumentParser();p.add_argument('--api',default='http://127.0.0.1:8006');p.add_argument('--out',type=Path,required=True);p.add_argument('--attach-run');p.add_argument('--timeout',type=int,default=7200);args=p.parse_args()
args.out.mkdir(parents=True,exist_ok=True)
def request(path,body=None):
 data=None if body is None else json.dumps(body).encode()
 req=urllib.request.Request(args.api+path,data=data,headers={'content-type':'application/json'})
 with urllib.request.urlopen(req,timeout=120) as response:return json.load(response)
run_id=args.attach_run
if run_id is None:run_id=request('/api/replay/runs',dict(dataset='full',rate=100000))['run_id']
start=time.monotonic();previous=None
with (args.out/'observations.jsonl').open('a',buffering=1) as stream:
 while True:
  state=request('/api/replay/current')
  if state['run_id']!=run_id:raise ValueError('Run identity changed')
  observation={key:state.get(key) for key in ['run_id','phase','confirmed','rate','lag','error','observation_error','historical_status','historical_application_id','published_revision']}
  observation.update(host_utc=datetime.now(timezone.utc).isoformat(),host_monotonic=time.monotonic())
  stream.write(json.dumps(observation)+'\n')
  marker=(state['phase'],state.get('confirmed',0)//100000)
  if marker!=previous: print(json.dumps(observation),flush=True);previous=marker
  if state['phase']=='failed':
   (args.out/'failed.json').write_text(json.dumps(state,indent=2));raise RuntimeError('Actual pipeline failed')
  if state['phase']=='completed' and state.get('historical_status')=='published':break
  if time.monotonic()-start>args.timeout:raise TimeoutError('Full pipeline timed out')
  time.sleep(5)
(args.out/'run.json').write_text(json.dumps(state,indent=2))
history=request('/api/history/overview');quality=request('/api/history/quality')
assert state['confirmed']==2756101,state['confirmed']
assert state['lag']==0 and state['stream_released'] is True
stream_quality=state['snapshot']['quality']
assert {key:stream_quality[key] for key in ['unique_source_count','valid_count','invalid_count','suspected_duplicate_count']}==dict(unique_source_count=2756101,valid_count=2756101,invalid_count=0,suspected_duplicate_count=460)
assert stream_quality['consumed_count']==2756101+stream_quality['retry_count']
assert stream_quality['consumed_count']==sum(state['final'].values())-sum(state['starting'].values())
assert history['snapshot']['batch_id']==run_id
assert history['data']['counts']==dict(view=2664312,addtocart=69332,transaction=22457)
assert history['data']['visitors']['visitors']==1407580
assert state['snapshot']['visitors']==1407580
assert quality['data']['batch_quality']['valid_count']==2756101
(args.out/'history.json').write_text(json.dumps(history,indent=2));(args.out/'quality.json').write_text(json.dumps(quality,indent=2))
(args.out/'summary.json').write_text(json.dumps(dict(accepted=True,run_id=run_id,published_revision=state['published_revision'],meaning='Real full registered producer→Kafka→incremental YARN→released→historical YARN→export/fetch/validated atomic publication; performance is separately tested'),indent=2))
print('FULL PIPELINE PASSED',run_id,flush=True)
