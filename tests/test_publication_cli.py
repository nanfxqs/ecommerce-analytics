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
        db.execute("CREATE TABLE events AS SELECT 'v1' AS dataset_version,'events.csv' AS source_file, i AS source_line, ? AS batch_id, 'visitor-1' AS visitorid, 'item-1' AS itemid, 'category-1' AS categoryid, 'view' AS event, TIMESTAMP '2015-05-04' AS event_time FROM range(2,4) t(i)", [batch])
        tables = {'events':'SELECT * FROM events', 'quarantine':'SELECT * FROM events WHERE false',
                  'raw':"SELECT to_json(events) AS value FROM events", 'quality':'SELECT * FROM quality',
                  'overview':"SELECT event,count(*) AS count FROM events GROUP BY event", 'visitor_events':"SELECT DATE '2015-05-04' AS event_date, 'visitor-1' AS visitorid, 'view' AS event",
                  'category_daily':"SELECT DATE '2015-05-04' AS event_date, 'category-1' AS categoryid, 2 AS events, 2 AS views, 0 AS addtocarts, 0 AS transactions",
                  'item_daily':"SELECT DATE '2015-05-04' AS event_date, 'item-1' AS itemid, 2 AS events, 2 AS views, 0 AS addtocarts, 0 AS transactions",
                  'visitor_daily':"SELECT DATE '2015-05-04' AS event_date, 'visitor-1' AS visitorid, 2 AS events, 0 AS purchases, TIMESTAMP '2015-05-04' AS last_event_time",
                  'visitor_category_daily':"SELECT DATE '2015-05-04' AS event_date, 'visitor-1' AS visitorid, 'category-1' AS categoryid, 2 AS views, 0 AS purchases"}
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


@pytest.mark.parametrize('table', ['overview', 'category_daily', 'item_daily', 'visitor_daily', 'visitor_category_daily', 'visitor_events'])
def test_publication_rejects_truncated_dashboard_results_and_retries(tmp_path, monkeypatch, table):
    store = tmp_path/'store'
    replay, good = complete_export(tmp_path, 'good')
    publish(monkeypatch, store, replay, good)
    replay, damaged = complete_export(tmp_path, 'damaged')
    path = damaged/table/'part.parquet'
    original = path.read_bytes()
    with duckdb.connect() as db:
        db.execute('CREATE TABLE empty AS SELECT * FROM read_parquet(?) WHERE false', [str(path)])
        db.execute('COPY empty TO ? (FORMAT PARQUET)', [str(path)])
    with pytest.raises(ValueError, match=table):
        publish(monkeypatch, store, replay, damaged)
    assert current_snapshot(store).batch_id == 'good'
    path.write_bytes(original)
    publish(monkeypatch, store, replay, damaged)
    assert current_snapshot(store).batch_id == 'damaged'


def test_publication_rejects_changed_category_key_despite_unchanged_totals(tmp_path, monkeypatch):
    replay, directory = complete_export(tmp_path, 'wrong-category')
    path = directory/'category_daily'/'part.parquet'
    with duckdb.connect() as db:
        db.execute("CREATE TABLE changed AS SELECT * REPLACE ('wrong-category' AS categoryid) FROM read_parquet(?)", [str(path)])
        db.execute('COPY changed TO ? (FORMAT PARQUET)', [str(path)])
    with pytest.raises(ValueError, match='category_daily'):
        publish(monkeypatch, tmp_path/'store', replay, directory)


def test_publication_accepts_all_invalid_inputs_with_empty_dashboard_results(tmp_path, monkeypatch):
    replay, directory = complete_export(tmp_path, 'all-invalid')
    metadata = json.loads((directory/'metadata.json').read_text())
    metadata['quality'].update(valid_count=0, invalid_count=2)
    (directory/'metadata.json').write_text(json.dumps(metadata))
    with duckdb.connect() as db:
        db.execute('CREATE TABLE invalid AS SELECT * FROM read_parquet(?)', [str(directory/'events'/'part.parquet')])
        db.execute('COPY invalid TO ? (FORMAT PARQUET)', [str(directory/'quarantine'/'part.parquet')])
        for table in ['events', 'overview', 'category_daily', 'item_daily', 'visitor_daily', 'visitor_category_daily', 'visitor_events']:
            path = directory/table/'part.parquet'
            db.execute('CREATE OR REPLACE TABLE empty AS SELECT * FROM read_parquet(?) WHERE false', [str(path)])
            db.execute('COPY empty TO ? (FORMAT PARQUET)', [str(path)])
        db.execute('CREATE TABLE quality AS SELECT * REPLACE (0 AS valid_count, 2 AS invalid_count) FROM read_parquet(?)', [str(directory/'quality'/'part.parquet')])
        db.execute('COPY quality TO ? (FORMAT PARQUET)', [str(directory/'quality'/'part.parquet')])
    publish(monkeypatch, tmp_path/'store', replay, directory)
    assert current_snapshot(tmp_path/'store').metadata['quality']['invalid_count'] == 2
