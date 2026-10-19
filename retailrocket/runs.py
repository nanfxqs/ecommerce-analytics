"""Durable local replay supervision over a single external cluster boundary."""
import fcntl
import json
import os
from pathlib import Path
import shlex
import subprocess
from threading import Event, RLock, Thread
from uuid import uuid4
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field


def atomic(path, value):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_suffix('.tmp')
    with temporary.open('w') as stream:
        json.dump(value,stream); stream.flush(); os.fsync(stream.fileno())
    os.replace(temporary,path)


class SSHCluster:
    """Only operator-selected remote worker commands cross SSH; no client paths."""
    def __init__(self, config): self.config=config
    def _call(self, operation, run=None):
        command=['env', 'PYTHONPATH='+self.config['code']+':'+self.config['runtime'], self.config['python'],'-m','retailrocket.edge_worker',operation,'--config',self.config['remote_config']]
        if run: command += ['--run-id',run['run_id']]
        result=subprocess.run(['ssh','-o','BatchMode=yes',self.config.get('host','h1'),shlex.join(command)],input=json.dumps(run or {}),text=True,capture_output=True,timeout=40)
        if result.returncode: raise RuntimeError(result.stderr[-2000:] or result.stdout[-2000:])
        return json.loads(result.stdout)
    def datasets(self): return self._call('datasets')
    def start(self,run): return self._call('start',run)
    def observe(self,run): return self._call('observe',run)


class Runs:
    def __init__(self, root, cluster, poll_seconds=2):
        self.root=Path(root); self.root.mkdir(parents=True,exist_ok=True)
        self.cluster=cluster; self.poll_seconds=poll_seconds; self.lock=RLock(); self.stop=Event()
        self.lease=(self.root/'supervisor.lock').open('a')
        fcntl.flock(self.lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
        self.path=self.root/'current.json'
        self.current=json.loads(self.path.read_text()) if self.path.exists() else None
        self.thread=Thread(target=self._supervise,daemon=True); self.thread.start()
    def close(self):
        self.stop.set(); self.thread.join(timeout=45); self.lease.close()
    def _save(self):
        atomic(self.path,self.current)
        atomic(self.root/self.current['run_id']/'run.json',self.current)
    def start(self,dataset,rate):
        with self.lock:
            if self.current and self.current['phase'] not in ('completed','stopped'): raise ValueError('已有活动或待恢复轮次')
            if dataset not in {d['id'] for d in self.cluster.datasets()}: raise KeyError('未知或未经校验的数据版本')
            self.current=dict(run_id='v2-'+uuid4().hex,dataset=dataset,requested_rate=rate,phase='starting',complete=False,confirmed=0,created_at=datetime.now(timezone.utc).isoformat())
            self._save()
            try: self.current.update(self.cluster.start(dict(self.current)))
            except Exception as error: self.current.update(phase='failed',error=str(error))
            self._save(); return dict(self.current)
    def read(self):
        with self.lock: return dict(self.current) if self.current else None
    def _supervise(self):
        while not self.stop.wait(self.poll_seconds):
            state=self.read()
            if not state or state['phase'] in ('completed','stopped','failed'): continue
            try:
                update=self.cluster.observe(state)
                ends=update.get('ends',{}); committed=update.get('committed',{})
                if any(committed.get(p,0)>end for p,end in ends.items()): raise ValueError('处理进度超出 Kafka 末端')
                update['lag']=sum(end-committed.get(p,0) for p,end in ends.items())
                update['updated_at']=datetime.now(timezone.utc).isoformat()
                with self.lock: self.current.update(update); self.current.pop('observation_error',None); self._save()
            except Exception as error:
                with self.lock: self.current['observation_error']=str(error); self._save()


class Start(BaseModel):
    dataset: str
    rate: float=Field(default=5,gt=0,le=1000000,allow_inf_nan=False)


def replay_router(runs):
    router=APIRouter(prefix='/api/replay')
    @router.get('/datasets')
    def datasets():
        try: return runs.cluster.datasets()
        except Exception as error: raise HTTPException(503,str(error)) from error
    @router.get('/current')
    def current(): return runs.read()
    @router.post('/runs',status_code=201)
    def start(request:Start):
        try: return runs.start(request.dataset,request.rate)
        except ValueError as error: raise HTTPException(409,str(error)) from error
        except KeyError as error: raise HTTPException(422,str(error)) from error
    return router
