#!/usr/bin/env python3
"""Real public-HTTP ACK-before-cursor crash/recovery check for a fresh mode profile."""
import argparse,json,time,urllib.request
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--api',default='http://127.0.0.1:8007');p.add_argument('--out',type=Path,required=True);p.add_argument('--fault-ack',type=int,default=200);args=p.parse_args();args.out.mkdir(parents=True,exist_ok=True)
def request(path,body=None):
 req=urllib.request.Request(args.api+'/api/replay/'+path,data=None if body is None else json.dumps(body).encode(),headers={'content-type':'application/json'})
 with urllib.request.urlopen(req,timeout=180) as response:return json.load(response)
def wait(predicate,seconds=360):
 deadline=time.monotonic()+seconds
 while True:
  state=request('current')
  with (args.out/'observations.jsonl').open('a') as target:target.write(json.dumps({k:state.get(k) for k in ['run_id','phase','confirmed','physical_messages','lag','error','committed_batch']})+'\n')
  if predicate(state):return state
  if time.monotonic()>deadline:raise TimeoutError(str(state))
  time.sleep(1)
run=request('runs',dict(dataset='small',rate=5));(args.out/'start.json').write_text(json.dumps(run,indent=2))
try:
 failed=wait(lambda s:s['phase']=='failed');time.sleep(1);failed=request('current');(args.out/'failed.json').write_text(json.dumps(failed,indent=2))
 assert failed['cursor']==args.fault_ack-1
 assert failed['stream_profile']['rocksdb_changelog'] is True
 original_messages=sum(failed['ends'].values());uncertain=original_messages-failed['cursor']
 assert 1<=uncertain<=5,(original_messages,failed['cursor'])
 assert failed['snapshot']['quality']['unique_source_count']>0,'Recovery must restore populated state'
 receipt=request('current/recover',{});(args.out/'recovery-receipt.json').write_text(json.dumps(receipt,indent=2))
 resumed=wait(lambda s:s['phase']=='running' and s['confirmed']>=350)
 request('current/stop',{});stopped=wait(lambda s:s['phase']=='stopped' and s.get('stream_released') is True)
 (args.out/'stopped.json').write_text(json.dumps(stopped,indent=2))
 assert stopped['run_id']==failed['run_id']==run['run_id']
 for key in ['topic','checkpoint','output','stream_profile']:assert stopped[key]==failed[key],key
 assert stopped['stream_pid']!=failed['stream_pid']
 n=stopped['confirmed'];quality=stopped['snapshot']['quality']
 assert stopped['lag']==0 and stopped['complete'] is False
 assert quality['unique_source_count']==quality['valid_count']==n
 assert quality['invalid_count']==0 and quality['retry_count']==uncertain
 assert quality['consumed_count']==n+uncertain==sum(stopped['final'].values())
 summary=dict(accepted=True,meaning='Populated exact state restored from SAME fresh changelog checkpoint after real ACK-before-cursor process death; intentional recovery excluded from performance',run_id=run['run_id'],profile=stopped['stream_profile'],fault_ack=args.fault_ack,persisted_cursor=failed['cursor'],physical_before=original_messages,resent_source_prefix=uncertain,final_source=n,final_physical=quality['consumed_count'],retry_count=quality['retry_count'],old_application=failed['snapshot']['application_id'],new_application=stopped['snapshot']['application_id'])
 (args.out/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary),flush=True)
except Exception:
 try:request('current/stop',{})
 except Exception:pass
 raise
