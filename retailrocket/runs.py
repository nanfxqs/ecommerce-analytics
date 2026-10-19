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
    def control(self,run,action): return self._call('control',dict(run,action=action))
    def _historical_command(self,operation,run):
        return ['ssh','-o','BatchMode=yes',self.config.get('host','h1'),shlex.join(['env','PYTHONPATH='+self.config['code']+':'+self.config['runtime'],self.config['python'],'-m','retailrocket.historical_worker',operation,'--config',self.config['remote_config'],'--run-id',run['run_id']])]
    def finalize(self,run):
        result=subprocess.run(self._historical_command('start',run),input=json.dumps(run),text=True,capture_output=True,timeout=40)
        if result.returncode: raise RuntimeError(result.stderr[-2000:] or result.stdout[-2000:])
        return json.loads(result.stdout)
    def fetch(self,run,directory):
        from retailrocket.finalization import fetch_bundle
        return fetch_bundle(self._historical_command('bundle',run),directory)


class Runs:
    def __init__(self, root, cluster, poll_seconds=2, publication_store=None, publication_checkpoint=None):
        self.root=Path(root); self.root.mkdir(parents=True,exist_ok=True)
        from retailrocket.finalization import Finalizer
        self.finalizer=Finalizer(publication_store, cluster, publication_checkpoint) if publication_store is not None and hasattr(cluster, 'finalize') else None
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
    def control(self, action):
        with self.lock:
            state = self.current
            if action not in ('pause', 'continue', 'stop'):
                raise ValueError('未知控制操作')
            if not state:
                raise ValueError('没有可控制的回放轮次')
            target = {'pause': 'paused', 'continue': 'running', 'stop': 'stopped'}[action]
            pending = state.get('pending_control')
            if pending and pending['action'] != action and action != 'stop':
                raise ValueError('已有控制请求等待后台确认')
            if (state['phase'] == target and not pending) or (pending and pending['action'] == action):
                return dict(accepted=True, action=action, duplicate=True, run_id=state['run_id'])
            allowed = {'pause': ('running',), 'continue': ('paused',), 'stop': ('running', 'paused')}
            if state['phase'] not in allowed[action] or (pending and pending['action'] == 'stop'):
                raise ValueError('当前状态不允许此操作')
            # Intent survives API failure even if the SSH acknowledgement is lost.
            intent = dict(action=action, operation_id=uuid4().hex)
            state['control_generation'] = state.get('control_generation', 0) + 1
            state['pending_control'] = intent
            self._save()
            try:
                result = self.cluster.control(dict(state), action)
            except Exception as error:
                state['control_error'] = str(error)
                self._save()
                raise RuntimeError('控制请求未确认，请检查轮次状态：' + str(error)) from error
            if ((result.get('last_control') or {}).get('operation_id') == intent['operation_id']
                    and result.get('phase') in ('running', 'paused', 'stopping', 'draining')):
                state.update({key: result[key] for key in ('phase','last_control','confirmed','cursor','rate') if key in result})
                if state['phase'] == target:
                    state.pop('pending_control', None)
            state.pop('control_error', None)
            self._save()
            return dict(result, run_id=state['run_id'], duplicate=False)

    def _historical_gate(self, state):
        from retailrocket.finalization import gate
        return gate(state, self.finalizer is not None)

    def retry_history(self):
        with self.lock:
            if not self.finalizer or not self.current or self.current.get('failure_stage') != 'history' or self.current['phase'] not in ('failed','recovery_required'):
                raise ValueError('当前轮次没有可重试的历史分析失败')
            self.current.update(phase='historical_pending',historical_status='pending',history_retry=True,error=None)
            self._save()
            return dict(accepted=True,run_id=self.current['run_id'])

    def read(self):
        with self.lock: return dict(self.current) if self.current else None
    def _supervise(self):
        while not self.stop.wait(self.poll_seconds):
            state=self.read()
            if not state or state['phase'] in ('completed','stopped','failed'): continue
            from retailrocket.finalization import HISTORY_PHASES
            if self.finalizer and state['phase'] in HISTORY_PHASES:
                try:
                    update=self.finalizer.advance(state,self.root/state['run_id'])
                except Exception as error:
                    update=dict(phase='failed',historical_status='failed',failure_stage='history',error=str(error),history_retry=False)
                update['updated_at']=datetime.now(timezone.utc).isoformat()
                with self.lock:
                    if self.current and self.current['run_id']==state['run_id'] and self.current['phase']==state['phase'] and not self.stop.is_set():
                        self.current.update(update); self._save()
                continue
            try:
                update=self.cluster.observe(state)
                ends=update.get('ends',{}); committed=update.get('committed',{})
                if any(committed.get(p,0)>end for p,end in ends.items()): raise ValueError('处理进度超出 Kafka 末端')
                update['lag']=sum(end-committed.get(p,0) for p,end in ends.items())
                update['updated_at']=datetime.now(timezone.utc).isoformat()
                with self.lock:
                    if (not self.current or self.current['run_id'] != state['run_id']
                            or self.current.get('control_generation', 0) != state.get('control_generation', 0)):
                        continue
                    self.current=self._historical_gate(dict(self.current,**update))
                    pending = self.current.get('pending_control')
                    if (pending and self.current['phase'] == {'pause':'paused','continue':'running','stop':'stopped'}[pending['action']]
                            and (update.get('last_control') or {}).get('operation_id') == pending['operation_id']
                            and (update.get('last_control') or {}).get('acknowledged') is True):
                        self.current.pop('pending_control', None)
                    self.current.pop('observation_error', None)
                    self._save()
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
    @router.post('/current/retry-history',status_code=202)
    def retry_history():
        try: return runs.retry_history()
        except ValueError as error: raise HTTPException(409,str(error)) from error
    @router.post('/current/{action}')
    def control(action: str):
        from fastapi.responses import JSONResponse
        try:
            result = runs.control(action)
            return JSONResponse(result, status_code=200 if result['duplicate'] else 202)
        except ValueError as error:
            raise HTTPException(409, str(error)) from error
        except RuntimeError as error:
            raise HTTPException(503, str(error)) from error
    return router
