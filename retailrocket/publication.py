"""Immutable local exports and an atomic pointer to the last successful batch.

The export is a downloaded result of Spark/HDFS processing, never its working
output. Consumers bind current_snapshot once for an entire dashboard request.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import uuid


def _identity(batch_id):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', batch_id):
        raise ValueError('batch_id must be a safe single path component')
    return batch_id


def _atomic(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with temporary.open('w') as stream:
            json.dump(value, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _inventory(directory):
    files = {}
    for path in sorted(directory.rglob('*')):
        if path.is_symlink():
            raise ValueError('Export must not contain symlinks')
        if path.is_file() and path != directory / 'publication.json':
            files[str(path.relative_to(directory))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return files


class BatchRun:
    """Persist each stage, including failure, independently of the current pointer."""
    def __init__(self, store, batch_id):
        self.store = Path(store)
        self.batch_id = _identity(batch_id)
        self.path = self.store / 'runs' / (batch_id + '.json')

    def stage(self, name, records=None, **details):
        state = json.loads(self.path.read_text()) if self.path.exists() else {'batch_id': self.batch_id, 'history': []}
        entry = dict(stage=name, at=datetime.now(timezone.utc).isoformat(), **details)
        if records is not None:
            entry['records'] = records
        state['history'].append(entry)
        state.update(status=name, **details)
        _atomic(self.path, state)

    def fail(self, error):
        self.stage('failed', error=str(error))

    @contextmanager
    def tracking(self, stage, **details):
        self.stage(stage, **details)
        try:
            yield self
        except Exception as error:
            self.fail(error)
            raise


@dataclass(frozen=True)
class Snapshot:
    batch_id: str
    directory: Path
    metadata: dict


def current_snapshot(store):
    store = Path(store)
    pointer = store / 'current.json'
    if not pointer.exists():
        raise FileNotFoundError('No successful batch has been published')
    current = json.loads(pointer.read_text())
    if not re.fullmatch(r'[0-9a-f]{64}', current['revision']):
        raise ValueError('Invalid published revision')
    directory = store / 'batches' / _identity(current['batch_id']) / current['revision']
    publication = json.loads((directory / 'publication.json').read_text())
    if _inventory(directory) != publication['files']:
        raise ValueError('Published export integrity check failed')
    return Snapshot(current['batch_id'], directory, json.loads((directory / 'metadata.json').read_text()))


def publish_batch(store, replay_manifest, export_directory, *, validate=None, before_switch=None):
    """Validate a complete export, then atomically publish; safe to retry.

    validate(snapshot) adds domain checks without coupling publication to metrics.
    before_switch(snapshot) is a fault-injection/operational checkpoint.
    """
    replay = json.loads(Path(replay_manifest).read_text())
    batch_id = _identity(replay['batch_id'])
    store, source = Path(store), Path(export_directory)
    run = BatchRun(store, batch_id)
    with run.tracking('validating', replay=replay, output=str(source)):
        if replay.get('replay_confirmed') is not True:
            raise ValueError('Replay is not confirmed; incomplete batches cannot publish')
        try:
            topic = replay['topic']
            starts = replay['startingOffsets'][topic]
            ends = replay['endingOffsets'][topic]
            if starts.keys() != ends.keys() or any(ends[p] < starts[p] for p in starts) or sum(ends[p] - starts[p] for p in starts) != replay['rows']:
                raise ValueError('Confirmed replay offset range differs from row count')
        except (KeyError, TypeError) as error:
            raise ValueError('Confirmed replay offset provenance is incomplete') from error
        if not source.is_dir():
            raise FileNotFoundError('Persisted export input is missing: ' + str(source))
        metadata = json.loads((source / 'metadata.json').read_text())
        if metadata.get('batch_id') != batch_id:
            raise ValueError('Export batch identity differs from replay')
        parquet = list(source.rglob('*.parquet'))
        if not parquet:
            raise ValueError('Export contains no Parquet results')
        import duckdb
        with duckdb.connect() as connection:
            for path in parquet:
                connection.execute('SELECT count(*) FROM read_parquet(?)', [str(path)]).fetchone()
        files = _inventory(source)
        revision = hashlib.sha256(json.dumps([replay, files], sort_keys=True).encode()).hexdigest()
        target = store / 'batches' / batch_id / revision
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            temporary = Path(tempfile.mkdtemp(prefix='.export-', dir=target.parent))
            try:
                shutil.copytree(source, temporary, dirs_exist_ok=True)
                if _inventory(temporary) != files:
                    raise ValueError('Export changed during publication')
                _atomic(temporary / 'publication.json', {'replay': replay, 'files': files})
                snapshot = Snapshot(batch_id, temporary, metadata)
                if validate:
                    validate(snapshot)
                os.rename(temporary, target)
            finally:
                if temporary.exists():
                    shutil.rmtree(temporary)
        elif _inventory(target) != files:
            raise ValueError('Existing immutable export is corrupt')
        snapshot = Snapshot(batch_id, target, metadata)
        if validate:
            validate(snapshot)
        run.stage('validated', records=replay.get('rows'), revision=revision)
        if before_switch:
            before_switch(snapshot)
        _atomic(store / 'current.json', {'batch_id': batch_id, 'revision': revision})
        run.stage('published', revision=revision)
        return snapshot
