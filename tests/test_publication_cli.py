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


def test_public_publish_rejects_duplicate_replacement_of_a_source_row(tmp_path, monkeypatch):
    replay, directory = complete_export(tmp_path, 'duplicate')
    path = directory/'events'/'part.parquet'
    with duckdb.connect() as db:
        db.execute('CREATE TABLE altered AS SELECT * REPLACE (2 AS source_line) FROM read_parquet(?)', [str(path)])
        db.execute('COPY altered TO ? (FORMAT PARQUET)', [str(path)])
    with pytest.raises(ValueError, match='Source completeness'):
        publish(monkeypatch, tmp_path/'store', replay, directory)
    with pytest.raises(FileNotFoundError, match='No successful batch'):
        current_snapshot(tmp_path/'store')


def test_spark_submission_failure_is_recorded_before_collecting(tmp_path):
    from retailrocket.spark_batch import run
    manifest = tmp_path/'replay.json'
    manifest.write_text(json.dumps(dict(batch_id='unconfirmed', replay_confirmed=False)))
    with pytest.raises(ValueError, match='not confirmed'):
        run(manifest, 'unreachable:9092', '/not-used', store=tmp_path/'store')
    state = json.loads((tmp_path/'store'/'runs'/'unconfirmed.json').read_text())
    assert state['status'] == 'failed'
    assert [stage['stage'] for stage in state['history']] == ['collecting', 'failed']


def test_fixed_inputs_to_spark_export_to_validated_publication(tmp_path, monkeypatch):
    from pyspark.sql import SparkSession
    from retailrocket.ingest import prepare, records
    from retailrocket.references import prepare_references
    from retailrocket.spark_batch import analyze
    from retailrocket.query import overview
    source = tmp_path/'events.csv'
    source.write_text('timestamp,visitorid,event,itemid,transactionid\n1430622000000,1,view,2,\nbad,1,view,2,\n')
    manifest = prepare(source, tmp_path/'prepared', 'fixture-v1')
    provenance = json.loads(manifest.read_text())
    props, tree = tmp_path/'properties.csv', tmp_path/'tree.csv'
    props.write_text('timestamp,itemid,property,value\n1000,2,categoryid,10\n')
    tree.write_text('categoryid,parentid\n10,\n')
    prepare_references([props], tree, tmp_path/'refs', 'fixture-v1', manifest)
    replay = dict(batch_id='complete', replay_confirmed=True, rows=2, input=provenance,
                  topic='events', startingOffsets={'events':{'0':0}}, endingOffsets={'events':{'0':2}})
    replay_path = tmp_path/'replay.json'
    replay_path.write_text(json.dumps(replay))
    messages = [dict(row, batch_id='complete') for row in records(manifest)]
    spark = SparkSession.builder.master('local[2]').appName('publication-contract').getOrCreate()
    spark.conf.set('spark.sql.shuffle.partitions','2')
    try:
        directory = tmp_path/'analyzed'
        analyze(spark.createDataFrame([(json.dumps(row),) for row in messages], 'value STRING'), replay,
                str(directory), references=str(tmp_path/'refs'))
        metadata = next((directory/'metadata').glob('part-*')).read_text()
        (directory/'metadata.json').write_text(metadata)
        publish(monkeypatch, tmp_path/'store', replay_path, directory)
        snapshot = current_snapshot(tmp_path/'store')
        assert overview(snapshot.directory/'events', '2015-05-03', '2015-05-03') == dict(view=1, addtocart=0, transaction=0)
        assert snapshot.metadata['quality']['invalid_count'] == 1
    finally:
        spark.stop()
