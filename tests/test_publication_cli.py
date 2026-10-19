"""The public publish command enforces the full analysis result contract."""
import json
import sys
import duckdb
import pytest
from retailrocket.cli import main
from retailrocket.publication import current_snapshot


def complete_export(tmp_path, batch):
    directory = tmp_path / batch
    directory.mkdir()
    source = dict(dataset_version='v1', source_file='events.csv', source_sha256='a'*64,
                  rows=2, first_source_line=2, last_source_line=3)
    replay = dict(batch_id=batch, replay_confirmed=True, rows=2, input=source, topic='events',
                  startingOffsets={'events': {'0': 0}}, endingOffsets={'events': {'0': 2}})
    manifest = tmp_path / (batch + '.json')
    manifest.write_text(json.dumps(replay))
    quality = dict(input_count=2, consumed_count=2, unique_source_count=2, valid_count=2,
                   invalid_count=0, retry_count=0, suspected_duplicate_count=0)
    metadata = dict(batch_id=batch, input=source, quality=quality,
                    references=dict(dataset_version='v1', events_input=source, sources=[dict(file='properties.csv',sha256='b'*64,rows=2),dict(file='tree.csv',sha256='c'*64,rows=1)]))
    (directory/'metadata.json').write_text(json.dumps(metadata))
    with duckdb.connect() as db:
        db.execute("CREATE TABLE events AS SELECT 'v1' AS dataset_version,'events.csv' AS source_file, i AS source_line, ? AS batch_id, 'view' AS event, TIMESTAMP '2015-05-04' AS event_time FROM range(2,4) t(i)", [batch])
        tables = {'events':'SELECT * FROM events', 'quarantine':'SELECT * FROM events WHERE false',
                  'raw':"SELECT to_json(events) AS value FROM events", 'quality':'SELECT * FROM quality',
                  'overview':"SELECT event,count(*) AS count FROM events GROUP BY event", 'visitor_events':'SELECT * FROM events',
                  'category_daily':'SELECT * FROM events', 'item_daily':'SELECT * FROM events',
                  'visitor_daily':'SELECT * FROM events', 'visitor_category_daily':'SELECT * FROM events'}
        q = tmp_path / (batch+'-quality.json')
        q.write_text(json.dumps(quality))
        db.execute('CREATE TABLE quality AS SELECT * FROM read_json_auto(?)', [str(q)])
        for name, sql in tables.items():
            (directory/name).mkdir()
            db.execute('COPY ('+sql+') TO ? (FORMAT PARQUET)', [str(directory/name/'part.parquet')])
    return manifest, directory


def publish(monkeypatch, store, replay, directory):
    monkeypatch.setattr(sys, 'argv', ['retailrocket','publish','--store',str(store),'--replay',str(replay),'--export',str(directory)])
    main()


def test_public_publish_rejects_incomplete_results_and_retries_safely(tmp_path, monkeypatch):
    store = tmp_path / 'store'
    first, directory = complete_export(tmp_path, 'first')
    publish(monkeypatch, store, first, directory)
    second, damaged = complete_export(tmp_path, 'second')
    metadata = json.loads((damaged/'metadata.json').read_text())
    metadata['quality']['valid_count'] = 3
    (damaged/'metadata.json').write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match='quality'):
        publish(monkeypatch, store, second, damaged)
    assert current_snapshot(store).batch_id == 'first'
    metadata['quality']['valid_count'] = 2
    (damaged/'metadata.json').write_text(json.dumps(metadata))
    publish(monkeypatch, store, second, damaged)
    publish(monkeypatch, store, second, damaged)
    assert current_snapshot(store).batch_id == 'second'
