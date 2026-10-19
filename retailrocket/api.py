"""Local HTTP queries over complete, immutable published analysis batches."""
from datetime import date, datetime, timezone
from pathlib import Path
from threading import RLock

from fastapi import FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder

from retailrocket.publication import current_snapshot
from retailrocket import query


class History:
    """Bind each response to one verified snapshot; invalidate on file changes."""
    def __init__(self, store):
        self.store = Path(store)
        self._lock = RLock()
        self._snapshot = None
        self._fingerprint = None
        self._responses = {}

    def _stamp(self):
        pointer = self.store / 'current.json'
        if not pointer.exists():
            return None
        # ctime/inode also detect replacements or edits that preserve size/mtime.
        paths = [pointer]
        if self._snapshot is not None:
            paths.extend(sorted(self._snapshot.directory.rglob('*')))
        return tuple((str(p), p.stat().st_ino, p.stat().st_size,
                      p.stat().st_mtime_ns, p.stat().st_ctime_ns) for p in paths if p.is_file())

    def snapshot(self):
        with self._lock:
            stamp = self._stamp()
            if stamp is None:
                return None
            if stamp != self._fingerprint:
                pointer = self.store / 'current.json'
                for _ in range(3):
                    identity = pointer.read_bytes()
                    candidate = current_snapshot(self.store)
                    # Publication may switch while integrity is being checked.
                    # Cache a revision only if its pointer stayed stable.
                    self._snapshot = candidate
                    fingerprint = self._stamp()
                    if pointer.read_bytes() == identity:
                        self._fingerprint = fingerprint
                        self._responses.clear()
                        break
                else:
                    raise ValueError('Publication changed during verification')
            return self._snapshot

    def overview(self, start=None, end=None):
        with self._lock:
            snapshot = self.snapshot()
            if snapshot is None:
                return {'snapshot': None, 'selected_range': None, 'data': None}
            key = (snapshot.directory, start, end)
            if key in self._responses:
                return self._responses[key]
            bounds = query.date_bounds(snapshot.directory)
            if (start is None) != (end is None):
                raise ValueError('Both UTC dates are required')
            start, end = (start, end) if start is not None else bounds
            if start is not None and start > end:
                raise ValueError('Start date must not follow end date')
            data = None if start is None else dict(
                counts=query.overview(snapshot.directory/'events', start, end),
                visitors=query.visitor_conversion(snapshot.directory, start, end),
                trend=query.daily_trend(snapshot.directory, start, end))
            result = jsonable_encoder(dict(snapshot=dict(
                batch_id=snapshot.batch_id, revision=snapshot.directory.name,
                timezone='UTC', complete=True,
                event_bounds=dict(start=bounds[0], end=bounds[1]),
                updated_at=datetime.fromtimestamp((snapshot.directory/'publication.json').stat().st_mtime, timezone.utc)),
                selected_range=dict(start=start, end=end), data=data))
            if len(self._responses) >= 128:
                self._responses.clear()
            self._responses[key] = result
            return result


def create_app(store, *, run_store=None, cluster=None, poll_seconds=2, publication_checkpoint=None):
    app = FastAPI(title='Retailrocket 历史数据回放')
    app.state.history = History(store)
    if cluster is not None:
        from retailrocket.runs import Runs, replay_router
        runs = Runs(run_store or Path(store).parent/'replay-runs', cluster, poll_seconds, publication_store=store, publication_checkpoint=publication_checkpoint)
        app.state.runs = runs
        app.include_router(replay_router(runs))
        app.add_event_handler('shutdown', runs.close)

    @app.get('/api/history/overview')
    def overview(start: date | None = Query(None), end: date | None = Query(None)):
        try:
            return app.state.history.overview(start, end)
        except ValueError as error:
            # Range errors are caller errors; publication errors are unavailable data.
            if str(error) in ('Both UTC dates are required', 'Start date must not follow end date'):
                raise HTTPException(422, str(error)) from error
            raise HTTPException(503, '已发布批次校验失败，请检查发布存储') from error
        except (FileNotFoundError, KeyError, OSError) as error:
            raise HTTPException(503, '已发布批次暂不可用，请检查发布存储') from error

    from retailrocket.history_pages import history_router
    app.include_router(history_router(app.state.history))
    return app


def main():
    """Explicit loopback binding; the browser never starts a Spark job."""
    import argparse
    import uvicorn
    parser = argparse.ArgumentParser()
    parser.add_argument('--store', required=True)
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--cluster-config')
    parser.add_argument('--poll-seconds',type=float,default=2,help='Serial background cache refresh delay; browser remains2seconds')
    args = parser.parse_args()
    if not 0 < args.poll_seconds <= 60:parser.error('--poll-seconds must be in(0,60]')
    cluster = None
    if args.cluster_config:
        import json
        from retailrocket.runs import SSHCluster
        cluster = SSHCluster(json.loads(Path(args.cluster_config).read_text()))
    uvicorn.run(create_app(args.store, cluster=cluster,poll_seconds=args.poll_seconds), host='127.0.0.1', port=args.port)


if __name__ == '__main__':
    main()
