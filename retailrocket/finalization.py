"""Sequential stream -> batch -> export -> validated publication orchestration."""
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
from uuid import uuid4
from retailrocket.publication import publish_batch
from retailrocket.validation import validate_analysis

HISTORY_PHASES = {'historical_pending', 'analyzing', 'exporting', 'publishing'}


def gate(state, enabled):
    if enabled and state.get('phase') == 'completed' and state.get('historical_status') != 'published':
        if not state.get('complete') or not state.get('stream_released') or state.get('early_stopped'):
            raise ValueError('完整回放及流资源释放尚未确认')
        ends=state.get('final') or state.get('ends')
        if not ends or any(state.get(name,{}).get(p,-1)<end for name in ('committed','result_offsets') for p,end in ends.items()):
            raise ValueError('最终检查点与实时结果尚未覆盖全部已发送数据')
        state.update(phase='historical_pending', historical_status='pending')
    return state


class Finalizer:
    def __init__(self, store, cluster, before_switch=None):
        self.store, self.cluster, self.before_switch = store, cluster, before_switch

    def advance(self, run, directory):
        if not run.get('complete') or not run.get('stream_released') or run.get('early_stopped'):
            raise ValueError('部分或未释放的回放不能发布历史分析')
        if run['phase'] in ('historical_pending', 'analyzing'):
            result = self.cluster.finalize(run)
            status = result.get('historical_status')
            if status == 'failed':
                raise RuntimeError(result.get('historical_error', '历史分析失败'))
            if status not in ('pending', 'analyzing', 'exporting', 'exported'):
                raise ValueError('未知历史分析阶段')
            return dict(result, phase='exporting' if status == 'exported' else 'analyzing', history_retry=bool(run.get('history_retry')) if status == 'pending' else False)
        if run['phase'] == 'exporting':
            replay, export = self.cluster.fetch(run, directory)
            return dict(phase='publishing', historical_status='publishing',
                        history_replay=str(replay), history_export=str(export))
        if run['phase'] == 'publishing':
            if json.loads(Path(run['history_replay']).read_text()).get('batch_id') != run['run_id']:
                raise ValueError('历史回放身份与轮次不一致')
            snapshot = publish_batch(self.store, run['history_replay'], run['history_export'],
                                     validate=validate_analysis, before_switch=self.before_switch)
            if snapshot.batch_id != run['run_id']:
                raise ValueError('历史批次与回放轮次不一致')
            return dict(phase='completed', historical_status='published', published_batch_id=snapshot.batch_id,
                        published_revision=snapshot.directory.name, error=None, failure_stage=None)
        raise ValueError('未知历史分析状态')


def fetch_bundle(command, directory):
    from retailrocket.cleanup import safe
    directory=safe(Path(directory)/('artifacts-'+uuid4().hex))
    directory.mkdir(parents=True,exist_ok=False)
    archive_path=directory/'transfer.tar'
    with archive_path.open('wb') as target:
        result=subprocess.run(command,stdout=target,stderr=subprocess.PIPE,timeout=600)
    if result.returncode: raise RuntimeError(result.stderr.decode(errors='replace')[-2000:])
    with tarfile.open(archive_path,mode='r|') as archive:
        for member in archive:
            parts=Path(member.name).parts
            if (not parts or parts[0] not in ('export','replay.json') or '..' in parts or Path(member.name).is_absolute()
                    or not (member.isdir() or member.isfile())):
                raise ValueError('不安全的历史导出文件')
            destination=safe(directory/member.name)
            if member.isdir(): destination.mkdir(parents=True,exist_ok=True)
            else:
                destination.parent.mkdir(parents=True,exist_ok=True)
                with archive.extractfile(member) as source, destination.open('xb') as target:
                    shutil.copyfileobj(source,target)
    archive_path.unlink()
    safe(directory,tree=True)
    return directory/'replay.json',directory/'export'
