"""Detached, run-owned YARN historical analysis after streaming resource release."""
import argparse
import fcntl
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import urllib.request
import xml.etree.ElementTree as ET
from uuid import uuid4
from retailrocket.cleanup import identity, safe, read, inactive_pid
from retailrocket.edge_worker import atomic


def hdfs(*args):
    result=subprocess.run(['/usr/local/hadoop/bin/hdfs','dfs',*args],capture_output=True,timeout=90)
    if result.returncode: raise RuntimeError(result.stderr.decode(errors='replace')[-2000:])
    return result.stdout


def digest(path):
    value=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''): value.update(block)
    return value.hexdigest()


def bound_references(config, replay):
    source=safe(config['reference_source'],tree=True)
    hashes=config['reference_sha256']
    names=('references.json','categories.csv','category_tree.csv')
    if set(hashes)!=set(names) or any(digest(source/name)!=hashes[name] for name in names):
        raise ValueError('Registered reference files changed')
    original=read(source/'references.json'); event=replay['input']; origin=original['events_input']
    if any(event.get(key)!=origin.get(key) for key in ('dataset_version','source_file','source_sha256')):
        raise ValueError('Reference and event source identities differ')
    if (not event['rows'] or event['last_source_line']-event['first_source_line']+1!=event['rows']
            or event['first_source_line']<origin['first_source_line'] or event['last_source_line']>origin['last_source_line']):
        raise ValueError('Reference binding requires a known contiguous input range')
    metadata=dict(original,events_input=event,binding_origin_sha256=hashes['references.json'])
    key=hashlib.sha256(json.dumps(metadata,sort_keys=True).encode()).hexdigest()
    local=safe(Path(config['root']).parent/'reference-bindings'/key,tree=True)
    if not local.exists():
        temporary=local.with_name(local.name+'.partial-'+uuid4().hex)
        temporary.mkdir(parents=True)
        for name in ('categories.csv','category_tree.csv'): shutil.copyfile(source/name,temporary/name)
        atomic(temporary/'references.json',metadata)
        os.rename(temporary,local)
    if read(local/'references.json')!=metadata or any(digest(local/name)!=hashes[name] for name in ('categories.csv','category_tree.csv')):
        raise ValueError('Immutable reference binding changed')
    remote='hdfs://mycluster/retailrocket/references/v2-'+key
    exists=subprocess.run(['/usr/local/hadoop/bin/hdfs','dfs','-test','-e',remote],capture_output=True)
    if exists.returncode==1 and not exists.stderr:
        temporary=remote+'.partial-'+uuid4().hex
        hdfs('-mkdir',temporary)
        for name in names: hdfs('-put',str(local/name),temporary+'/'+name)
        hdfs('-mv',temporary,remote)
    elif exists.returncode:
        raise RuntimeError('Reference binding existence is uncertain')
    if json.loads(hdfs('-cat',remote+'/references.json'))!=metadata:
        raise ValueError('HDFS reference binding identity mismatch')
    for name in ('categories.csv','category_tree.csv'):
        if hashlib.sha256(hdfs('-cat',remote+'/'+name)).hexdigest()!=hashes[name]:
            raise ValueError('HDFS reference binding checksum mismatch')
    return remote



def yarn_released(config,app_id):
    addresses=config.get('yarn_webapps')
    if addresses is None:
        directory=Path(os.environ.get('HADOOP_CONF_DIR','/usr/local/hadoop/etc/hadoop'))
        properties={p.findtext('name'):p.findtext('value') for p in ET.parse(directory/'yarn-site.xml').findall('property')}
        prefix='yarn.resourcemanager.webapp.address'
        addresses=['http://'+value for key,value in properties.items() if key==prefix or key.startswith(prefix+'.')]
    if not addresses: raise ValueError('ResourceManager web addresses are not configured')
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    errors=[]
    for address in addresses:
        try:
            with opener.open(address.rstrip('/')+'/ws/v1/cluster/apps/'+app_id,timeout=3) as response:
                application=json.load(response)['app']
            if application.get('id')!=app_id: raise ValueError('YARN application identity mismatch')
            return application.get('state')=='FINISHED' and application.get('finalStatus')=='SUCCEEDED'
        except Exception as error: errors.append(str(error))
    raise RuntimeError('Cannot confirm YARN application release: '+'; '.join(errors))

def ready(config, run_id):
    root=safe(Path(config['root'])/identity(run_id),tree=True)
    state=read(root/'state.json'); replay=read(root/'replay.json')
    if (state.get('run_id')!=run_id or state.get('phase')!='completed' or not state.get('complete')
            or state.get('early_stopped') or state.get('stream_released') is not True):
        raise ValueError('Only complete, released streaming runs can enter history')
    if replay.get('batch_id')!=run_id or replay.get('replay_confirmed') is not True or replay['input']!=read(root/'input.json'):
        raise ValueError('Historical input identity mismatch')
    inactive_pid(read(root/'launcher.json')['pid'])
    if 'stream_pid' in state: inactive_pid(state['stream_pid'])
    observed=read(root/'stream-observation.json'); snapshot=observed.get('snapshot') or {}
    app_id=snapshot.get('application_id','')
    if snapshot.get('run_id')!=run_id or not re.fullmatch(r'application_[0-9]+_[0-9]+',app_id):
        raise ValueError('Streaming application identity is missing')
    final=state.get('final',{})
    if not final or any(observed.get(name,{}).get(p,-1)<end for name in ('committed','result_offsets') for p,end in final.items()):
        raise ValueError('Final streaming result and checkpoint do not cover replay')
    if not yarn_released(config,app_id):
        raise ValueError('Streaming YARN resources are not confirmed released')
    return root,replay


def start(config,run):
    root=safe(Path(config['root'])/identity(run['run_id']),tree=True)
    with (root/'history-launch.lock').open('a') as launch:
        fcntl.flock(launch,fcntl.LOCK_EX)
        path=root/'history.json'
        state=read(path) if path.exists() else None
        if state and state['historical_status'] in ('exported',): return state
        if state and state['historical_status'] in ('analyzing','exporting'):
            try: inactive_pid(state['pid'])
            except ValueError: return state
            state.update(historical_status='failed',historical_error='Historical worker exited before completion')
            atomic(path,state)
        if state and state['historical_status']=='failed' and not run.get('history_retry'): return state
        try:
            root,replay=ready(config,run['run_id'])
        except ValueError as error:
            if str(error) in ('Recorded worker or stream process is still active','Streaming YARN resources are not confirmed released'):
                return dict(historical_status='pending')
            raise
        # The streaming worker may have written complete immediately before closing its lease.
        lease=(root/'worker.lock').open('a')
        try:
            try: fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError: return dict(historical_status='pending')
        finally: lease.close()
        log=(root/'history.log').open('a')
        state=dict(historical_status='analyzing',run_id=run['run_id'],attempt=(state or {}).get('attempt',0)+1)
        atomic(path,state)
        process=subprocess.Popen([sys.executable,'-m','retailrocket.historical_worker','work','--config',config['_file'],'--run-id',run['run_id']],stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        state['pid']=process.pid; atomic(path,state)
        return state


def work(config,run_id):
    root,replay=ready(config,run_id)
    with (root/'history-launch.lock').open('a') as launch:
        fcntl.flock(launch,fcntl.LOCK_EX)
        state=read(root/'history.json')
    with (root/'worker.lock').open('a') as lease:
        fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:
            references=bound_references(config,replay)
            output='hdfs://mycluster/retailrocket/batches/'+run_id
            evidence=root/'spark-evidence.json'
            raw=subprocess.run(['/usr/local/hadoop/bin/hdfs','dfs','-test','-e',output+'/raw/_SUCCESS'],capture_output=True)
            if raw.returncode not in (0,1) or raw.stderr: raise RuntimeError('Batch raw input existence is uncertain')
            command=[str(Path(config['code'])/'scripts/submit-batch.sh'),'--replay',str(root/'replay.json'),'--brokers',config['brokers'],'--output',output,'--references',references,'--store',str(root/'batch-state')]
            if raw.returncode==0: command.append('--retry-from-raw')
            subprocess.run(command,check=True)
            state.update(historical_status='exporting',historical_application_id=read(evidence)['application_id'])
            atomic(root/'history.json',state)
            export=root/'export'
            if not export.exists(): subprocess.run([str(Path(config['code'])/'scripts/export-batch.sh'),output,str(export),str(evidence)],check=True)
            if read(export/'metadata.json').get('batch_id')!=run_id: raise ValueError('Export batch identity mismatch')
            state.update(historical_status='exported')
            atomic(root/'history.json',state)
        except Exception as error:
            state.update(historical_status='failed',historical_error=str(error))
            atomic(root/'history.json',state)


def main():
    parser=argparse.ArgumentParser(); parser.add_argument('operation',choices=['start','work','bundle']); parser.add_argument('--config',required=True); parser.add_argument('--run-id',required=True)
    args=parser.parse_args(); config=read(args.config); config['_file']=str(Path(args.config).resolve()); run_id=identity(args.run_id)
    if args.operation=='start': print(json.dumps(start(config,dict(json.load(sys.stdin),run_id=run_id))))
    elif args.operation=='work': work(config,run_id)
    else:
        root=safe(Path(config['root'])/run_id,tree=True)
        if read(root/'history.json').get('historical_status')!='exported': raise ValueError('Export is not complete')
        with tarfile.open(fileobj=sys.stdout.buffer,mode='w|') as archive:
            archive.add(root/'replay.json',arcname='replay.json',recursive=False)
            archive.add(root/'export',arcname='export')


if __name__=='__main__': main()
