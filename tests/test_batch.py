import json
from pathlib import Path
from retailrocket.ingest import prepare, records


def test_fixed_input_preserves_source_lines_and_event_time(tmp_path):
    source = tmp_path / 'events.csv'
    source.write_text('timestamp,visitorid,event,itemid,transactionid\n1430622000000,1,view,2,\n1430622000001,1,addtocart,2,\n1430622000002,1,transaction,2,9\n')
    manifest = prepare(source, tmp_path / 'prepared', 'fixture-v1', limit=2)
    rows = list(records(manifest))
    assert [(r['source_line'], r['event'], r['timestamp']) for r in rows] == [(2, 'view', '1430622000000'), (3, 'addtocart', '1430622000001')]
    assert rows[0]['source_record_id'] != rows[1]['source_record_id']
    assert json.loads(Path(manifest).read_text())['rows'] == 2


def test_dashboard_reads_exported_overview(tmp_path):
    import duckdb
    from retailrocket.query import overview
    conn = duckdb.connect()
    conn.execute("CREATE TABLE events(event VARCHAR,event_time TIMESTAMP)")
    conn.execute("INSERT INTO events VALUES ('view','2015-05-03'),('view','2015-05-04'),('addtocart','2015-05-04'),('transaction','2015-05-04')")
    conn.execute(f"COPY events TO '{tmp_path / 'events.parquet'}' (FORMAT PARQUET)")
    assert overview(tmp_path, '2015-05-04', '2015-05-04') == {'view': 1, 'addtocart': 1, 'transaction': 1}


def test_prepared_batch_retains_original_input(tmp_path):
    from retailrocket.ingest import records
    source = tmp_path / 'events.csv'
    original = 'timestamp,visitorid,event,itemid,transactionid\n1430622000000,1,view,2,\n'
    source.write_text(original)
    manifest = prepare(source, tmp_path / 'prepared', 'fixture-v1')
    metadata = json.loads(Path(manifest).read_text())
    assert (Path(manifest).parent / metadata['raw_source']).read_text() == original
