"""Explicit, offline cleanup of terminal replay runs. Dry-run unless --apply."""
import argparse
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile

TERMINAL = {'completed', 'stopped', 'failed'}


def identity(value):
    if not re.fullmatch(r'v2-[0-9a-f]{32}', value):
        raise ValueError('Unknown or unsafe replay run identity')
    return value


def safe(path, *, tree=False):
    """Reject symlinks before resolving, including every existing ancestor."""
    path = Path(path).absolute()
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError('Symlink paths cannot be cleaned')
    if tree and path.is_dir():
        if any(child.is_symlink() for child in path.rglob('*')):
            raise ValueError('Symlink in owned artifact tree')
    return path


def read(path):
    safe(path)
    value = json.loads(Path(path).read_text())
    if not isinstance(value, dict):
        raise ValueError('Unknown state format')
    return value


@contextmanager
def lease(path):
    safe(path)
    with Path(path).open('a') as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError('Active supervisor or worker; stop it before cleanup') from error
        yield


def terminal(state, run_id):
    if state.get('run_id') != run_id or state.get('phase') not in TERMINAL:
        raise ValueError('Active, unknown or uncertain replay run')


class Remote:
    def cleanup(self, config, run_id, protected, apply):
        command = ['env', 'PYTHONPATH='+config['code']+':'+config['runtime'],
                   config['python'], '-m', 'retailrocket.cleanup', '--edge-config',
                   config['remote_config'], '--run-id', run_id]
        if apply:
            command.append('--apply')
        result = subprocess.run(['ssh', '-o', 'BatchMode=yes', config.get('host', 'h1'), shlex.join(command)],
                                input=json.dumps({'protected_batch': protected}), text=True,
                                capture_output=True, timeout=180)
        if result.returncode:
            raise RuntimeError(result.stderr[-2000:] or result.stdout[-2000:])
        return json.loads(result.stdout)


def local_cleanup(run_store, store, config, run_id, apply, remote):
    run_id = identity(run_id)
    root, store = safe(run_store), safe(store, tree=True)
    if not root.is_dir() or not store.is_dir():
        raise ValueError('Run and publication stores must already exist')
    owned = safe(root/run_id, tree=True)
    if not owned.is_dir():
        raise ValueError('Unknown replay run')
    with lease(root/'supervisor.lock'):
        terminal(read(owned/'run.json'), run_id)
        safe(root/'current.json')
        current_run = read(root/'current.json') if (root/'current.json').exists() else None
        if current_run:
            identity(current_run.get('run_id', ''))
            if not isinstance(current_run.get('phase'), str):
                raise ValueError('Unknown current replay state')
        if current_run and current_run.get('run_id') == run_id:
            terminal(current_run, run_id)
        protected = None
        if (store/'current.json').exists():
            # Full verification fails closed on corrupt/unknown publication state.
            from retailrocket.publication import current_snapshot
            snapshot = current_snapshot(store)
            if snapshot.metadata.get('batch_id') != snapshot.batch_id:
                raise ValueError('Published batch identity mismatch')
            protected = snapshot.batch_id
        elif (store/'batches').exists() and any((store/'batches').iterdir()):
            raise ValueError('Missing publication pointer with existing batches')
        if run_id == protected:
            raise ValueError('Current published batch and revision cannot be cleaned')
        paths = [owned]
        batch = safe(store/'batches'/run_id, tree=True)
        if batch.exists():
            if not batch.is_dir():
                raise ValueError('Unknown batch artifact')
            for revision in batch.iterdir():
                if not re.fullmatch(r'[0-9a-f]{64}', revision.name) or not revision.is_dir():
                    raise ValueError('Unknown published revision')
                if read(revision/'metadata.json').get('batch_id') != run_id:
                    raise ValueError('Batch artifact identity mismatch')
            paths.append(batch)
        batch_state = safe(store/'runs'/(run_id+'.json'))
        if batch_state.exists():
            if read(batch_state).get('batch_id') != run_id:
                raise ValueError('Batch state identity mismatch')
            paths.append(batch_state)
        receipt_path = owned/'cleanup-receipt.json'
        receipt = read(receipt_path) if receipt_path.exists() else None
        if receipt is None:
            receipt = remote.cleanup(read(config), run_id, protected, apply)
        if receipt.get('run_id') != run_id or not isinstance(receipt.get('applied'), bool) or (apply and receipt['applied'] is not True):
            raise ValueError('Unknown remote cleanup receipt')
        result = dict(run_id=run_id, applied=apply, paths=[str(p) for p in paths], remote=receipt)
        if apply:
            # An interrupted local deletion can retry using the durable edge receipt.
            safe(owned, tree=True)
            persist_receipt(receipt_path, receipt)
            for path in paths[1:]:
                safe(path, tree=True)
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
            if current_run and current_run.get('run_id') == run_id:
                (root/'current.json').unlink()
            safe(owned, tree=True)
            shutil.rmtree(owned)
        return result



def persist_receipt(path, value):
    safe(path)
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, prefix='.cleanup-', delete=False) as stream:
        temporary = Path(stream.name)
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def external(config, kind, args):
    command = config.get(kind+'_command', ['/usr/local/hadoop/bin/'+kind])
    if not isinstance(command, list) or not command or not all(isinstance(s, str) for s in command):
        raise ValueError('Invalid operator-selected external command')
    return subprocess.run([*command, *args], text=True, capture_output=True, timeout=60)


def inactive_pid(value):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 1:
        raise ValueError('Unknown process identity')
    try:
        os.kill(value, 0)
    except ProcessLookupError:
        return
    except PermissionError as error:
        raise ValueError('Process ownership is uncertain') from error
    raise ValueError('Recorded worker or stream process is still active')


def edge_cleanup(config, run_id, apply, guard):
    run_id = identity(run_id)
    if 'protected_batch' not in guard:
        raise ValueError('Missing local publication protection')
    protected = guard['protected_batch']
    if protected is not None and (not isinstance(protected, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', protected)):
        raise ValueError('Unknown local publication protection')
    if protected == run_id:
        raise ValueError('Current published batch cannot be cleaned')
    root = safe(config['root'])
    owned = safe(root/run_id, tree=True)
    receipts = safe(root/'.cleanup-receipts', tree=True)
    receipt_path = safe(receipts/(run_id+'.json'))
    if not owned.is_dir():
        if receipt_path.exists():
            receipt = read(receipt_path)
            if receipt.get('run_id') == run_id and receipt.get('applied') is True:
                return dict(receipt, applied=apply)
        raise ValueError('Unknown edge replay run')
    with lease(owned/'worker.lock'):
        state = read(owned/'state.json')
        terminal(state, run_id)
        if read(owned/'request.json').get('run_id') != run_id:
            raise ValueError('Edge request identity mismatch')
        inactive_pid(read(owned/'launcher.json').get('pid'))
        if 'stream_pid' in state:
            inactive_pid(state['stream_pid'])
        if state['phase'] in ('completed', 'stopped') and state.get('stream_released') is not True:
            raise ValueError('Streaming resource release is uncertain')
        paths = ['hdfs://mycluster/retailrocket/'+name+'/'+run_id
                 for name in ('streams', 'stream-checkpoints', 'batches')]
        if state.get('output') != paths[0] or state.get('checkpoint') != paths[1]:
            raise ValueError('Unknown or unsafe HDFS ownership')
        if (owned/'replay.json').exists() and read(owned/'replay.json').get('batch_id') != run_id:
            raise ValueError('Replay identity mismatch')
        applications = external(config, 'yarn', ['application', '-list', '-appStates', 'NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING'])
        if applications.returncode or re.search(r'application_\d+_\d+', applications.stdout) or not re.search(r'Total number of applications[^\n]*:\s*0\s*$', applications.stdout, re.MULTILINE):
            raise ValueError('Active or uncertain YARN applications; retry when cluster is idle')
        present = []
        for path in paths:
            # Inspect each HDFS ancestor without following a run-owned alias.
            segments = path.removeprefix('hdfs://mycluster/').split('/')
            for count in range(1, len(segments)):
                ancestor = 'hdfs://mycluster/'+'/'.join(segments[:count])
                listing = external(config, 'hdfs', ['dfs', '-ls', '-d', ancestor])
                if listing.returncode or not any(line.startswith('d') for line in listing.stdout.splitlines()) or any(line.startswith('l') for line in listing.stdout.splitlines()):
                    raise ValueError('Unknown or symlink HDFS ancestor')
            exists = external(config, 'hdfs', ['dfs', '-test', '-e', path])
            if exists.returncode == 1 and not exists.stderr.strip():
                continue
            if exists.returncode:
                raise ValueError('HDFS existence is uncertain')
            direct = external(config, 'hdfs', ['dfs', '-ls', '-d', path])
            if direct.returncode or not any(line.startswith('d') for line in direct.stdout.splitlines()) or any(line.startswith('l') for line in direct.stdout.splitlines()):
                raise ValueError('Unknown or symlink HDFS artifact')
            listing = external(config, 'hdfs', ['dfs', '-ls', '-R', path])
            if listing.returncode or any(line.startswith('l') for line in listing.stdout.splitlines()):
                raise ValueError('Unknown or symlink HDFS artifact')
            present.append(path)
        result = dict(run_id=run_id, applied=apply, paths=[*present, str(owned)])
        if apply:
            for path in present:
                removed = external(config, 'hdfs', ['dfs', '-rm', '-r', path])
                if removed.returncode:
                    raise RuntimeError('HDFS cleanup failed; local artifacts retained')
            receipts.mkdir(exist_ok=True)
            persist_receipt(receipt_path, result)
            safe(owned, tree=True)
            shutil.rmtree(owned)
        return result

def main(argv=None, *, remote=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--run-store')
    parser.add_argument('--store')
    parser.add_argument('--cluster-config')
    parser.add_argument('--edge-config')
    args = parser.parse_args(argv)
    if args.edge_config:
        result = edge_cleanup(read(args.edge_config), args.run_id, args.apply, json.load(sys.stdin))
    else:
        if not all((args.run_store, args.store, args.cluster_config)):
            parser.error('--run-store, --store and --cluster-config are required for local cleanup')
        result = local_cleanup(args.run_store, args.store, args.cluster_config, args.run_id, args.apply, remote or Remote())
    print(json.dumps(result))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, RuntimeError, OSError, KeyError) as error:
        print('Cleanup refused: '+str(error), file=sys.stderr)
        raise SystemExit(2)
