"""Run-owned edge worker: Kafka ACK cursor, YARN stream and HDFS observations.

All arguments except server-generated run identity originate in operator config.
"""
import argparse
from collections import deque
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

def atomic(path, value):
    path = Path(path)
    temporary = path.with_suffix('.tmp')
    with temporary.open('w') as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def identity(value):
    if not re.fullmatch(r'v2-[0-9a-f]{32}',value): raise ValueError('Invalid run identity')
    return value


def directory(config,run_id): return Path(config['root']).resolve()/identity(run_id)


def checksum(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def dataset(config,key):
    entry=config['datasets'][key]
    manifest=Path(entry['manifest']).resolve()
    metadata=json.loads(manifest.read_text())
    events=(manifest.parent/metadata['events']).resolve()
    if events.parent!=manifest.parent: raise ValueError('Prepared input escaped registry root')
    if checksum(events)!=entry['events_sha256']: raise ValueError('Prepared events checksum mismatch')
    source=(manifest.parent/metadata['raw_source']).resolve()
    if source.parent!=manifest.parent: raise ValueError('Source escaped registry root')
    digest=hashlib.sha256()
    with source.open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''): digest.update(block)
    if digest.hexdigest()!=metadata['source_sha256']: raise ValueError('Source checksum mismatch')
    count=0
    with events.open() as stream:
        for count,line in enumerate(stream,1):
            row=json.loads(line)
            if row['source_line']!=count+1 or row['source_file']!=metadata['source_file'] or row['dataset_version']!=metadata['dataset_version']:
                raise ValueError('Prepared provenance mismatch')
            expected=hashlib.sha256(f"{metadata['dataset_version']}:{metadata['source_sha256']}:{metadata['source_file']}:{count+1}".encode()).hexdigest()
            if row['source_record_id']!=expected: raise ValueError('Source record identity mismatch')
    if count!=metadata['rows']: raise ValueError('Prepared row count mismatch')
    return manifest,metadata,events


def kafka(config):
    from kafka import KafkaConsumer
    return KafkaConsumer(bootstrap_servers=config['brokers'].split(','),enable_auto_commit=False,request_timeout_ms=15000,api_version_auto_timeout_ms=10000)


def ends(consumer,topic):
    from kafka import TopicPartition
    parts=consumer.partitions_for_topic(topic)
    if not parts: raise ValueError('Run topic missing')
    return {str(p.partition):offset for p,offset in consumer.end_offsets([TopicPartition(topic,p) for p in sorted(parts)]).items()}


def hdfs(*args,missing=False):
    result=subprocess.run(['/usr/local/hadoop/bin/hdfs','dfs',*args],capture_output=True,text=True,timeout=30)
    if result.returncode and not missing: raise RuntimeError(result.stderr[-1500:])
    return result.stdout if result.returncode==0 else ''


def children(path):
    return [line.split()[-1] for line in hdfs('-ls',path,missing=True).splitlines() if line.startswith('d') or line.startswith('-')]


def observation(config,run_id):
    root=directory(config,run_id); state=json.loads((root/'state.json').read_text())
    consumer=kafka(config)
    try: state['ends']=ends(consumer,state['topic'])
    finally: consumer.close()
    cached = root/'stream-observation.json'
    observed = json.loads(cached.read_text()) if cached.exists() else None
    if observed:
        snapshot = observed['snapshot']
        if snapshot['run_id'] != run_id:
            raise ValueError('Snapshot identity mismatch')
        if set(observed['committed']) != set(state['ends']):
            raise ValueError('Checkpoint partition mismatch')
        state.update(observed)
    else:
        state.update(committed={p:0 for p in state['ends']}, result_offsets={}, samples=[], snapshot=None)

    return state


def start(config,run):
    root=directory(config,run['run_id']); root.mkdir(parents=True,exist_ok=False)
    atomic(root/'request.json',run)
    log=(root/'worker.log').open('a')
    process=subprocess.Popen([sys.executable,'-m','retailrocket.edge_worker','work','--config',config['_file'],'--run-id',run['run_id']],stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    atomic(root/'launcher.json',dict(pid=process.pid))
    return dict(phase='starting',confirmed=0)


def work(config,run_id):
    root=directory(config,run_id)
    lease=(root/'worker.lock').open('a'); fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
    run=json.loads((root/'request.json').read_text())
    state=dict(phase='starting',run_id=run_id,topic=run_id,confirmed=0,cursor=0,rate=0,
               output='hdfs://mycluster/retailrocket/streams/'+run_id,
               checkpoint='hdfs://mycluster/retailrocket/stream-checkpoints/'+run_id,complete=False)
    atomic(root/'state.json',state)
    stream=None; producer=None; consumer=None
    try:
        manifest,metadata,events=dataset(config,run['dataset'])
        from kafka import KafkaProducer
        from kafka.admin import KafkaAdminClient,NewTopic
        admin=KafkaAdminClient(bootstrap_servers=config['brokers'].split(','))
        try: admin.create_topics([NewTopic(run_id,num_partitions=3,replication_factor=3)])
        finally: admin.close()
        consumer=kafka(config); starting=ends(consumer,run_id)
        atomic(root/'input.json',metadata)
        command=[str(Path(config['code'])/'scripts/submit-stream.sh'),'--brokers',config['brokers'],'--topic',run_id,'--run-id',run_id,'--output',state['output'],'--checkpoint',state['checkpoint'],'--max-offsets',str(config.get('max_offsets',1000)),'--stop-file',str(root/'stop-stream'),'--observation-file',str(root/'stream-observation.json')]
        log=(root/'stream.log').open('a')
        environment=dict(os.environ); environment['RETAILROCKET_SHUFFLE_PARTITIONS']='2'
        stream=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,env=environment,start_new_session=True)
        state.update(phase='running',stream_pid=stream.pid,starting=starting,input=metadata)
        atomic(root/'state.json',state)
        producer=KafkaProducer(bootstrap_servers=config['brokers'].split(','),acks='all',retries=10,value_serializer=lambda row:json.dumps(row).encode(),linger_ms=20,batch_size=65536)
        began=time.monotonic(); pending=deque(); confirmed=0
        def acknowledge():
            nonlocal confirmed
            position,future=pending.popleft(); future.get(timeout=120)
            confirmed+=1
            state.update(cursor=position,confirmed=confirmed,rate=confirmed/max(.001,time.monotonic()-began))
            atomic(root/'state.json',state)
        with events.open() as source:
            for position,line in enumerate(source,1):
                if stream.poll() is not None: raise RuntimeError('YARN streaming driver exited before producer completion')
                row=json.loads(line); row.update(batch_id=run_id,replay_time=datetime.now(timezone.utc).isoformat())
                target=began+(position-1)/run['requested_rate']
                if target>time.monotonic(): time.sleep(target-time.monotonic())
                pending.append((position,producer.send(run_id,key=row['source_record_id'].encode(),value=row)))
                if len(pending)>=min(1000,max(1,int(run['requested_rate']))): acknowledge()
            while pending: acknowledge()
        producer.flush(timeout=120); final=ends(consumer,run_id)
        replay=dict(batch_id=run_id,topic=run_id,rows=sum(final[p]-starting[p] for p in final),replay_confirmed=True,startingOffsets={run_id:starting},endingOffsets={run_id:final},input=metadata)
        atomic(root/'replay.json',replay)
        state.update(phase='draining',final=final); atomic(root/'state.json',state)
        deadline=time.monotonic()+config.get('drain_timeout',1800)
        while True:
            if stream.poll() is not None: raise RuntimeError('Stream exited before committed drain')
            observed=observation(config,run_id)
            if all(observed['committed'].get(p,0)>=end and observed['result_offsets'].get(p,0)>=end for p,end in final.items()): break
            if time.monotonic()>deadline: raise TimeoutError('Timed out waiting for committed stream drain')
            time.sleep(2)
        (root/'stop-stream').touch()
        stream.wait(timeout=120)
        if stream.returncode: raise RuntimeError('Streaming shutdown failed')
        state.update(phase='completed',complete=True,stream_released=True)
        atomic(root/'state.json',state)
    except Exception as error:
        state.update(phase='failed',error=str(error)); atomic(root/'state.json',state)
        if stream and stream.poll() is None:
            (root/'stop-stream').touch()
            try: stream.wait(timeout=60)
            except subprocess.TimeoutExpired: os.killpg(stream.pid,signal.SIGTERM)
    finally:
        if producer: producer.close(timeout=30)
        if consumer: consumer.close()
        lease.close()


def main():
    parser=argparse.ArgumentParser(); parser.add_argument('operation',choices=['datasets','start','observe','work']); parser.add_argument('--config',required=True); parser.add_argument('--run-id')
    args=parser.parse_args(); config=json.loads(Path(args.config).read_text()); config['_file']=str(Path(args.config).resolve())
    if args.operation=='datasets':
        # Checksum validation occurs at start; registry entries are operator-prevalidated.
        result=[dict(id=key,label=value['label'],rows=json.loads(Path(value['manifest']).read_text())['rows']) for key,value in config['datasets'].items()]
    elif args.operation=='start': result=start(config,json.load(sys.stdin))
    elif args.operation=='observe': result=observation(config,identity(args.run_id))
    else: work(config,identity(args.run_id)); return
    print(json.dumps(result))


if __name__=='__main__': main()
