"""Fail-closed takeover of one existing replay identity and checkpoint."""
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from uuid import uuid4, UUID


def process_identity(pid):
    try:
        stat=Path(f'/proc/{pid}/stat').read_text().split(') ',1)[1].split()
        return dict(pid=pid,start=stat[19],boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip())
    except FileNotFoundError:return None


def alive(record):
    if not record or process_identity(record['pid']) != record:return False
    try:return Path(f"/proc/{record['pid']}/stat").read_text().split(') ',1)[1].split()[0]!='Z'
    except FileNotFoundError:return False


def worker_alive(root):
    with (root/'worker.lock').open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return True
        return False


def quiesce(config, run):
    from retailrocket.edge_worker import directory,atomic,observation
    root=directory(config,run['run_id'])
    with (root/'control.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        state=json.loads((root/'state.json').read_text())
        if worker_alive(root) and state['phase'] in ('starting','running'):
            atomic(root/'control.json',dict(action='pause',operation_id='quiesce-'+str(run.get('control_generation',0))))
    observed=observation(config,run['run_id'])
    observed['worker_alive']=worker_alive(root)
    return observed


def checkpoint(config, root, state):
    from retailrocket.edge_worker import hdfs
    # Derived identities prevent a client or stale file from selecting another run.
    run_id=state['run_id']
    if state['checkpoint']!='hdfs://mycluster/retailrocket/stream-checkpoints/'+run_id or state['output']!='hdfs://mycluster/retailrocket/streams/'+run_id or state['topic']!=run_id:
        raise ValueError('Recovery resource identity mismatch')
    try:
        metadata=json.loads(hdfs('-cat',state['checkpoint']+'/metadata'))
        UUID(metadata['id'])
        cached=root/'stream-observation.json'
        if cached.exists():
            observed=json.loads(cached.read_text())
            if observed['snapshot']['run_id']!=run_id or observed['snapshot'].get('engine')!='exact-state-v1':
                raise ValueError('Checkpoint result engine/identity mismatch')
            batch=observed['committed_batch']
            commit=hdfs('-cat',state['checkpoint']+'/commits/'+str(batch)).splitlines()
            if len(commit)!=2 or commit[0]!='v1':raise ValueError('Invalid checkpoint commit log')
            json.loads(commit[1])
            offset=hdfs('-cat',state['checkpoint']+'/offsets/'+str(batch)).splitlines()
            if len(offset)<3 or offset[0]!='v1':raise ValueError('Invalid checkpoint offset log')
            json.loads(offset[1])
            for source in offset[2:]:
                values=json.loads(source)
                if set(values)!={run_id} or values[run_id]!=observed['committed']:
                    raise ValueError('Checkpoint offset identity mismatch')
    except Exception as error:
        raise ValueError('Original checkpoint missing or corrupt; refusing reset: '+str(error)) from error


def retire_driver(root,state):
    record=state.get('stream_identity')
    if not record:
        raise ValueError('Missing verified driver process identity; refusing unsafe takeover')
    if not alive(record):return
    command=Path(f"/proc/{record['pid']}/cmdline").read_bytes().decode(errors='replace')
    if state['run_id'] not in command or 'spark' not in command.lower():
        raise ValueError('Driver process no longer belongs to replay run')
    (root/'stop-stream').touch()
    deadline=time.monotonic()+45
    while alive(record) and time.monotonic()<deadline:time.sleep(.2)
    if alive(record):
        os.killpg(record['pid'],signal.SIGTERM)
        deadline=time.monotonic()+15
        while alive(record) and time.monotonic()<deadline:time.sleep(.2)
    if alive(record):raise RuntimeError('Owned stream has not released; recovery refused')


def recover(config,run):
    from retailrocket.edge_worker import directory,atomic,control,observation
    root=directory(config,run['run_id'])
    with (root/'recovery.lock').open('a') as lease:
        fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
        state=json.loads((root/'state.json').read_text())
        if state['run_id']!=run['run_id']:raise ValueError('Recovery run identity mismatch')
        if state['phase'] in ('completed','stopped') and state.get('stream_released'):
            return observation(config,run['run_id'])
        checkpoint(config,root,state)
        if worker_alive(root):
            if state['phase']=='paused':
                request=dict(run,action='continue',pending_control=dict(action='continue',operation_id=uuid4().hex))
                control(config,request)
            elif state['phase'] not in ('draining','stopping'):
                raise RuntimeError('Live worker not quiesced yet; retry manual recovery')
            return observation(config,run['run_id'])
        retire_driver(root,state)
        # No live producer remains; final-range intent and ACK cursor are retained.
        stop=root/'stop-stream'
        if stop.exists():stop.unlink()
        current=json.loads((root/'control.json').read_text()) if (root/'control.json').exists() else None
        if not current or current['action']!='stop':
            atomic(root/'control.json',dict(action='continue',operation_id=uuid4().hex))
        log=(root/'worker.log').open('a')
        process=subprocess.Popen([sys.executable,'-m','retailrocket.edge_worker','work','--config',config['_file'],'--run-id',run['run_id'],'--resume'],stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        atomic(root/'launcher.json',process_identity(process.pid))
        return dict(phase='starting',recovered=True,complete=False)
