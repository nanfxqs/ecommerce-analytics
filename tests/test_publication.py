import json
import pytest
from retailrocket.publication import publish_batch, current_snapshot
from retailrocket.query import overview


def export(tmp_path, batch, count):
    import duckdb
    path = tmp_path / batch
    (path / 'events').mkdir(parents=True)
    with duckdb.connect() as db:
        db.execute("CREATE TABLE events AS SELECT 'view' AS event, TIMESTAMP '2015-05-04' AS event_time FROM range(?)", [count])
        db.execute('COPY events TO ? (FORMAT PARQUET)', [str(path / 'events' / 'part.parquet')])
    (path / 'metadata.json').write_text(json.dumps({'batch_id': batch, 'data_range': ['2015-05-04', '2015-05-04']}))
    replay = tmp_path / (batch + '.json')
    replay.write_text(json.dumps({'batch_id': batch, 'replay_confirmed': True, 'rows': count, 'input': {'dataset_version': 'v1', 'source_file': 'events.csv'}, 'topic': 'events', 'startingOffsets': {'events': {'0': 0}}, 'endingOffsets': {'events': {'0': count}}}))
    return replay, path


def test_retry_replaces_successful_pointer_without_appending_or_mixing(tmp_path):
    store = tmp_path / 'published'
    replay, directory = export(tmp_path, 'first', 2)
    publish_batch(store, replay, directory)
    bound = current_snapshot(store)
    replay2, directory2 = export(tmp_path, 'second', 3)
    def fail(snapshot):
        raise RuntimeError('injected before publication')
    with pytest.raises(RuntimeError, match='injected'):
        publish_batch(store, replay2, directory2, before_switch=fail)
    assert current_snapshot(store).batch_id == 'first'
    publish_batch(store, replay2, directory2)
    publish_batch(store, replay2, directory2)
    assert overview(current_snapshot(store).directory / 'events', '2015-05-04', '2015-05-04')['view'] == 3
    assert overview(bound.directory / 'events', '2015-05-04', '2015-05-04')['view'] == 2


def test_incomplete_or_missing_persisted_export_never_publishes(tmp_path):
    store = tmp_path / 'store'
    replay, directory = export(tmp_path, 'batch', 1)
    details = json.loads(replay.read_text())
    details['replay_confirmed'] = False
    replay.write_text(json.dumps(details))
    with pytest.raises(ValueError, match='not confirmed'):
        publish_batch(store, replay, directory)
    with pytest.raises(FileNotFoundError, match='No successful batch'):
        current_snapshot(store)
    details['replay_confirmed'] = True
    replay.write_text(json.dumps(details))
    with pytest.raises(FileNotFoundError, match='Persisted export input is missing'):
        publish_batch(store, replay, tmp_path / 'missing')
    assert json.loads((store / 'runs' / 'batch.json').read_text())['status'] == 'failed'
